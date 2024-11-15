/*
 * Copyright 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/future.hh>
#include <seastar/http/httpd.hh>
#include "seastarx.hh"
#include <seastar/json/json_elements.hh>
#include <seastar/core/sharded.hh>

#include "service/migration_manager.hh"
#include "service/client_state.hh"
#include "service_permit.hh"
#include "db/timeout_clock.hh"

#include "alternator/error.hh"
#include "stats.hh"
#include "utils/rjson.hh"
#include "utils/updateable_value.hh"
#include "utils/dynamodb/client.hh"
#include "utils/dynamodb/creds.hh"
#include "service/storage_proxy.hh"
#include <regex>

namespace db {
    class system_distributed_keyspace;
}

namespace query {
class partition_slice;
class result;
}

namespace cql3::selection {
    class selection;
}

namespace service {
    class storage_proxy;
}

namespace cdc {
    class metadata;
}

namespace gms {

class gossiper;

}

namespace alternator {

class rmw_operation;

struct make_jsonable : public json::jsonable {
    rjson::value _value;
public:
    explicit make_jsonable(rjson::value&& value);
    std::string to_json() const override;
};

/**
 * Make return type for serializing the object "streamed",
 * i.e. direct to HTTP output stream. Note: only useful for
 * (very) large objects as there are overhead issues with this
 * as well, but for massive lists of return objects this can
 * help avoid large allocations/many re-allocs
 */ 
json::json_return_type make_streamed(rjson::value&&);

struct json_string : public json::jsonable {
    std::string _value;
public:
    explicit json_string(std::string&& value);
    std::string to_json() const override;
};

namespace parsed {
class path;
};

schema_ptr get_table(service::storage_proxy& proxy, const rjson::value& request);
bool is_alternator_keyspace(const sstring& ks_name);
// Wraps the db::get_tags_of_table and throws if the table is missing the tags extension.
const std::map<sstring, sstring>& get_tags_of_table_or_throw(schema_ptr schema);

// An attribute_path_map object is used to hold data for various attributes
// paths (parsed::path) in a hierarchy of attribute paths. Each attribute path
// has a root attribute, and then modified by member and index operators -
// for example in "a.b[2].c" we have "a" as the root, then ".b" member, then
// "[2]" index, and finally ".c" member.
// Data can be added to an attribute_path_map using the add() function, but
// requires that attributes with data not be *overlapping* or *conflicting*:
//
// 1. Two attribute paths which are identical or an ancestor of one another
//    are considered *overlapping* and not allowed. If a.b.c has data,
//    we can't add more data in a.b.c or any of its descendants like a.b.c.d.
//
// 2. Two attribute paths which need the same parent to have both a member and
//    an index are considered *conflicting* and not allowed. E.g., if a.b has
//    data, you can't add a[1]. The meaning of adding both would be that the
//    attribute a is both a map and an array, which isn't sensible.
//
// These two requirements are common to the two places where Alternator uses
// this abstraction to describe how a hierarchical item is to be transformed:
//
// 1. In ProjectExpression: for filtering from a full top-level attribute
//    only the parts for which user asked in ProjectionExpression.
//
// 2. In UpdateExpression: for taking the previous value of a top-level
//    attribute, and modifying it based on the instructions in the user
//    wrote in UpdateExpression.

template<typename T>
class attribute_path_map_node {
public:
    using data_t = T;
    // We need the extra unique_ptr<> here because libstdc++ unordered_map
    // doesn't work with incomplete types :-(
    using members_t =  std::unordered_map<std::string, std::unique_ptr<attribute_path_map_node<T>>>;
    // The indexes list is sorted because DynamoDB requires handling writes
    // beyond the end of a list in index order.
    using indexes_t = std::map<unsigned, std::unique_ptr<attribute_path_map_node<T>>>;
    // The prohibition on "overlap" and "conflict" explained above means
    // That only one of data, members or indexes is non-empty.
    std::optional<std::variant<data_t, members_t, indexes_t>> _content;

    bool is_empty() const { return !_content; }
    bool has_value() const { return _content && std::holds_alternative<data_t>(*_content); }
    bool has_members() const { return _content && std::holds_alternative<members_t>(*_content); }
    bool has_indexes() const { return _content && std::holds_alternative<indexes_t>(*_content); }
    // get_members() assumes that has_members() is true
    members_t& get_members() { return std::get<members_t>(*_content); }
    const members_t& get_members() const { return std::get<members_t>(*_content); }
    indexes_t& get_indexes() { return std::get<indexes_t>(*_content); }
    const indexes_t& get_indexes() const { return std::get<indexes_t>(*_content); }
    T& get_value() { return std::get<T>(*_content); }
    const T& get_value() const { return std::get<T>(*_content); }
};

template<typename T>
using attribute_path_map = std::unordered_map<std::string, attribute_path_map_node<T>>;

using attrs_to_get_node = attribute_path_map_node<std::monostate>;
// attrs_to_get lists which top-level attribute are needed, and possibly also
// which part of the top-level attribute is really needed (when nested
// attribute paths appeared in the query).
// Most code actually uses optional<attrs_to_get>. There, a disengaged
// optional means we should get all attributes, not specific ones.
using attrs_to_get = attribute_path_map<std::monostate>;


class executor : public peering_sharded_service<executor> {
    gms::gossiper& _gossiper;
    service::storage_proxy& _proxy;
    service::migration_manager& _mm;
    db::system_distributed_keyspace& _sdks;
    cdc::metadata& _cdc_metadata;
    // An smp_service_group to be used for limiting the concurrency when
    // forwarding Alternator request between shards - if necessary for LWT.
    smp_service_group _ssg;

public:
    using client_state = service::client_state;
    using request_return_type = std::variant<json::json_return_type, api_error>;
    stats _stats;
    void trace_table_access(
        table_ops_type op, 
        const std::string& table_name,
        std::string user_name,
        const std::chrono::steady_clock::duration& latency = std::chrono::steady_clock::duration::zero()
    );
    static constexpr auto ATTRS_COLUMN_NAME = ":attrs";
    static constexpr auto KEYSPACE_NAME_PREFIX = "alternator_";
    static constexpr std::string_view INTERNAL_TABLE_PREFIX = ".scylla.alternator.";

    executor(gms::gossiper& gossiper,
             service::storage_proxy& proxy,
             service::migration_manager& mm,
             db::system_distributed_keyspace& sdks,
             cdc::metadata& cdc_metadata,
             smp_service_group ssg,
             utils::updateable_value<uint32_t> default_timeout_in_ms,
             const std::vector<std::string_view> &user_list = {})
        : _gossiper(gossiper), _proxy(proxy), _mm(mm), _sdks(sdks), _cdc_metadata(cdc_metadata), _ssg(ssg) {
        s_default_timeout_in_ms = std::move(default_timeout_in_ms);
        for (auto& user : user_list) {
            s_user_list.insert(sstring(user));
        }
    }

    future<request_return_type> create_table(client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value request);
    future<request_return_type> describe_table(client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value request);
    future<request_return_type> delete_table(client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value request);
    future<request_return_type> update_table(client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value request);
    future<request_return_type> put_item(client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value request);
    future<request_return_type> get_item(client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value request);
    future<request_return_type> delete_item(client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value request);
    future<request_return_type> update_item(client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value request);
    future<request_return_type> list_tables(client_state& client_state, service_permit permit, rjson::value request);
    future<request_return_type> scan(client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value request);
    future<request_return_type> describe_endpoints(client_state& client_state, service_permit permit, rjson::value request, std::string host_header);
    future<request_return_type> batch_write_item(client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value request);
    future<request_return_type> batch_get_item(client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value request);
    future<request_return_type> query(client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value request);
    future<request_return_type> tag_resource(client_state& client_state, service_permit permit, rjson::value request);
    future<request_return_type> untag_resource(client_state& client_state, service_permit permit, rjson::value request);
    future<request_return_type> list_tags_of_resource(client_state& client_state, service_permit permit, rjson::value request);
    future<request_return_type> update_time_to_live(client_state& client_state, service_permit permit, rjson::value request);
    future<request_return_type> describe_time_to_live(client_state& client_state, service_permit permit, rjson::value request);
    future<request_return_type> list_streams(client_state& client_state, service_permit permit, rjson::value request);
    future<request_return_type> describe_stream(client_state& client_state, service_permit permit, rjson::value request);
    future<request_return_type> get_shard_iterator(client_state& client_state, service_permit permit, rjson::value request);
    future<request_return_type> get_records(client_state& client_state, tracing::trace_state_ptr, service_permit permit, rjson::value request);
    future<request_return_type> describe_continuous_backups(client_state& client_state, service_permit permit, rjson::value request);

    future<> start();
    future<> stop() {
        // disconnect from the value source, but keep the value unchanged.
        s_default_timeout_in_ms = utils::updateable_value<uint32_t>{s_default_timeout_in_ms()};
        return make_ready_future<>();
    }

    static sstring table_name(const schema&);
    static db::timeout_clock::time_point default_timeout();
    static db::timeout_clock::time_point default_timeout(db::timeout_clock::duration timeout);
    static void set_default_timeout(db::timeout_clock::duration timeout);
    static void set_default_getitem_timeout(db::timeout_clock::duration timeout);
    static void set_default_putitem_nonlwt_timeout(db::timeout_clock::duration timeout);
    static void set_default_putitem_lwt_timeout(db::timeout_clock::duration timeout);
    static void set_default_query_timeout(db::timeout_clock::duration timeout);
    static void set_default_write_consistency_level(std::string_view cl);
    static void set_default_read_consistency_level(std::string_view cl);
    static void set_default_getrecords_consistency_level(std::string_view cl);
    static void set_default_query_consistency_level(std::string_view cl);
    static void set_alternator_replication_factor(int rf);
    static db::consistency_level default_write_consistency_level;
    static db::consistency_level default_write_consistency_level_lwt;
    static db::consistency_level default_read_consistency_level;
    static db::consistency_level default_getrecords_consistency_level;
    static db::consistency_level default_query_consistency_level;

    static db::timeout_clock::duration s_default_timeout;
    static db::timeout_clock::duration default_getitem_timeout;
    static db::timeout_clock::duration default_putitem_nonlwt_timeout;
    static db::timeout_clock::duration default_putitem_lwt_timeout;
    static db::timeout_clock::duration default_query_timeout;
    static int alternator_replication_factor;
private:
    static thread_local utils::updateable_value<uint32_t> s_default_timeout_in_ms;
public:
    static thread_local std::set<sstring> s_user_list;
    static schema_ptr find_table(service::storage_proxy&, const rjson::value& request);

private:
    friend class rmw_operation;

    static void describe_key_schema(rjson::value& parent, const schema&, std::unordered_map<std::string,std::string> * = nullptr);
    
public:
    static void describe_key_schema(rjson::value& parent, const schema& schema, std::unordered_map<std::string,std::string>&);

    static std::optional<rjson::value> describe_single_item(schema_ptr,
        const query::partition_slice&,
        const cql3::selection::selection&,
        const query::result&,
        const std::optional<attrs_to_get>&);

    static future<std::vector<rjson::value>> describe_multi_item(schema_ptr schema,
        const query::partition_slice&& slice,
        shared_ptr<cql3::selection::selection> selection,
        foreign_ptr<lw_shared_ptr<query::result>> query_result,
        shared_ptr<const std::optional<attrs_to_get>> attrs_to_get);

    static future<std::vector<rjson::value>> transfer_query_result_to_json(schema_ptr schema,
        const query::partition_slice& slice,
        shared_ptr<cql3::selection::selection> selection,
        query::result& query_result,
        std::optional<attrs_to_get>& attrs_to_get);

    static void describe_single_item(const cql3::selection::selection&,
        const std::vector<managed_bytes_opt>&,
        const std::optional<attrs_to_get>&,
        rjson::value&,
        bool = false);

    static void add_stream_options(const rjson::value& stream_spec, schema_builder&, service::storage_proxy& sp);
    static void supplement_table_info(rjson::value& descr, const schema& schema, service::storage_proxy& sp);
    static void supplement_table_stream_info(rjson::value& descr, const schema& schema, const service::storage_proxy& sp);
};

// is_big() checks approximately if the given JSON value is "bigger" than
// the given big_size number of bytes. The goal is to *quickly* detect
// oversized JSON that, for example, is too large to be serialized to a
// contiguous string - we don't need an accurate size for that. Moreover,
// as soon as we detect that the JSON is indeed "big", we can return true
// and don't need to continue calculating its exact size.
// For simplicity, we use a recursive implementation. This is fine because
// Alternator limits the depth of JSONs it reads from inputs, and doesn't
// add more than a couple of levels in its own output construction.
bool is_big(const rjson::value& val, int big_size = 100'000);

void validate_table_name(const std::string& name);

extern const std::unordered_map<service::storage_proxy::synctable_column_alter_method, std::function<void(rjson::value&)>> alter_method_map;
extern const std::unordered_map<service::storage_proxy::synctable_column_filter_method, std::function<bool(const rjson::value&)>> filter_method_map;

// item format: { "id": {"N": "1"}, "name": {"S": "John"} }
// git rid of non-key attributes from write_item to format read_item.
inline rjson::value get_read_item_from_write_item(const rjson::value& write_item, const std::vector<sstring>& target_table_keys) {
    rjson::value read_item = rjson::empty_object();
    int key_size = target_table_keys.size();
    int curr_size = 0;
    for (auto it = write_item.MemberBegin(); it != write_item.MemberEnd(); ++it) {
        std::string_view key = rjson::to_string_view(it->name);
        if (std::find(target_table_keys.begin(), target_table_keys.end(), key) != target_table_keys.end()) {
            rjson::add_with_string_name(read_item, key, rjson::copy(it->value));
            curr_size++;
            if (curr_size == key_size) {
                break;
            }
        }
    }
    return read_item;
}

// batch write syntax
// {                             (batch_write_obj)
//   "RequestItems": {           (request_items_obj)
//     "MyTable": [              (table_name_arr)
//       {                       (put_request_frame_obj)
//         "PutRequest": {       (put_request_obj)
//           "Item": {           (for (auto& item : items))
//             "id": {"N": "1"},
//             "name": {"S": "John"}
//           }
//         }
//       },
//       {
//         "PutRequest": {
//           "Item": {
//             "id": {"N": "2"},
//             "name": {"S": "Jane"}
//           }
//         }
//       }
//     ]
//   }
// }

// batch get syntax
// {                                                  (batch_read_obj)
//    "RequestItems": {                               (read_request_items_obj)
//       "MyTable" : {                                (read_table_name_obj)
//          "AttributesToGet": [ "id", "name" ],      (attrs_to_get_arr, same as target_table_keys)
//          "ConsistentRead": false,
//          "Keys": [                                 (read_keys_arr)
//             {                                      (modified item， filter according to target_table_keys)
//                "id": {"N": "1"},
//                "name": {"S": "John"}
//             },
//             { 
//                "id": {"N": "2"},
//                "name": {"S": "Jane"}
//             }
//          ]
//       }
//    }
// }
template <typename Iterator>
SEASTAR_CONCEPT( requires requires (Iterator i) {
     *i++;
     { i != i } -> std::convertible_to<bool>;
     std::same_as<std::remove_reference_t<decltype(*i)>, rapidjson::Value>;
} )
// we must format read and write json togather, bacause the key used to read or write may be altered (replace_shard_id_to_zero for example)
// during the following code.
inline std::tuple<rjson::value, rjson::value> get_batch_write_read_item_format_impl(Iterator start, Iterator end,
    std::string_view table_name, std::vector<sstring>& target_table_keys, bool read_before_write,
    const std::vector<std::pair<sstring, service::storage_proxy::synctable_column_alter_method>>& altered_columns,
    const std::vector<std::pair<sstring, service::storage_proxy::synctable_column_filter_method>>& filtered_columns) {
    // batch write json value
    rjson::value batch_write_obj = rjson::empty_object();
    rjson::value request_items_obj = rjson::empty_object();
    rjson::value table_name_arr = rjson::empty_array();

    // batch read json value
    rjson::value batch_read_obj = rjson::empty_object();
    rjson::value read_request_items_obj = rjson::empty_object();
    rjson::value read_table_name_obj = rjson::empty_object();
    // rjson::value attrs_to_get_arr = rjson::empty_array();
    rjson::value read_keys_arr = rjson::empty_array();

    // we need to get all data from the peer database for comparation to decide whether to send the write request. so don't use AttributesToGet.
    // for (auto key : target_table_keys) {
    //     rjson::value json_key = rjson::from_string(key);
    //     rjson::push_back(attrs_to_get_arr, std::move(json_key));
    // }

    Iterator curr = start;

    while (curr != end) {
        rjson::value& item = *curr++;
        rjson::value put_request_frame_obj = rjson::empty_object();
        rjson::value put_request_obj = rjson::empty_object();
        bool filtered = false;

        for (auto& filtered_column : filtered_columns) {
            // find the column which need to check its filter rule.
            auto column_iter = item.FindMember(filtered_column.first);
            if (column_iter != item.MemberEnd()) {
                rjson::value& data = (column_iter->value).MemberBegin()->value;
                // if filter return true, is means that this record will be filtered and do not need to be synced.
                if (filter_method_map.at(filtered_column.second)(data)) {
                    filtered = true;
                    break;
                }
            }
        }

        if (filtered) {
            continue;
        }

        for (auto& altered_column : altered_columns) {
            auto column_iter = item.FindMember(altered_column.first);
            if (column_iter != item.MemberEnd()) {
                rjson::value& data = (column_iter->value).MemberBegin()->value;
                alter_method_map.at(altered_column.second)(data);
            }
        }

        if (read_before_write) {
            rjson::value read_item = get_read_item_from_write_item(item, target_table_keys);
            rjson::push_back(read_keys_arr, std::move(read_item));
        }

        rjson::add_with_string_name(put_request_obj, "Item", std::move(item));
        rjson::add_with_string_name(put_request_frame_obj, "PutRequest", std::move(put_request_obj));
        rjson::push_back(table_name_arr, std::move(put_request_frame_obj));
    }

    if (read_before_write) {
        // rjson::add_with_string_name(read_table_name_obj, "AttributesToGet", std::move(attrs_to_get_arr));
        rjson::add_with_string_name(read_table_name_obj, "ConsistentRead", rjson::value(false));
        rjson::add_with_string_name(read_table_name_obj, "Keys", std::move(read_keys_arr));
        rjson::add_with_string_name(read_request_items_obj, table_name, std::move(read_table_name_obj));
        rjson::add_with_string_name(batch_read_obj, "RequestItems", std::move(read_request_items_obj));
    }

    rjson::add_with_string_name(request_items_obj, table_name, std::move(table_name_arr));
    rjson::add_with_string_name(batch_write_obj, "RequestItems", std::move(request_items_obj));
    // need to use std::move to form tuple.
    return {std::move(batch_write_obj), std::move(batch_read_obj)};
}

// items: [{"id": {"N": "1"}, "name": {"S": "John"}}, {"id": {"N": "2"}, "name": {"S": "Jane"}}]
inline std::tuple<rjson::value, rjson::value> get_batch_write_read_item_format(rjson::value&& items,
    std::string_view table_name, std::vector<sstring>& target_table_keys, bool read_before_write,
    const std::vector<std::pair<sstring, service::storage_proxy::synctable_column_alter_method>>& altered_columns,
    const std::vector<std::pair<sstring, service::storage_proxy::synctable_column_filter_method>>& filter_columns) {
    if (!items.IsArray()) {
        throw std::invalid_argument("Items has invalid format.");
    }

    auto curr = items.GetArray().begin();
    auto end = items.GetArray().end();
    return get_batch_write_read_item_format_impl(curr, end, table_name, target_table_keys, read_before_write, altered_columns, filter_columns);
}

// items: <{"id": {"N": "1"}, "name": {"S": "John"}}, {"id": {"N": "2"}, "name": {"S": "Jane"}}>
inline std::tuple<rjson::value, rjson::value> get_batch_write_read_item_format(std::vector<rjson::value>&& items,
    std::string_view table_name, std::vector<sstring>& target_table_keys, bool read_before_write,
    const std::vector<std::pair<sstring, service::storage_proxy::synctable_column_alter_method>>& altered_columns,
    const std::vector<std::pair<sstring, service::storage_proxy::synctable_column_filter_method>>& filter_columns) {
    auto curr = items.begin();
    auto end = items.end();
    return get_batch_write_read_item_format_impl(curr, end, table_name, target_table_keys, read_before_write, altered_columns, filter_columns);
}
}
