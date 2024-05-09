#pragma once

#include <seastar/core/file.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/metrics.hh>
#include "utils/http.hh"

#include "utils/dynamodb/creds.hh"

using namespace seastar;
class memory_data_sink_buffers;

namespace dynamodb {

using dynamodb_clock = std::chrono::steady_clock;

future<> ignore_reply(const http::reply& rep, input_stream<char>&& in_);

class client : public enable_shared_from_this<client> {
public:
    enum class opcode_type {
        put_item = 0,
        batch_write_item,
    };

private:
    std::unordered_map<opcode_type, std::string_view> opcode_map {
        {opcode_type::put_item, "PutItem"},
        {opcode_type::batch_write_item, "BatchWriteItem"},
    };

    class upload_sink_base;
    class upload_sink;
    class upload_jumbo_sink;
    class readable_file;

    std::string _host;
    endpoint_config_ptr _cfg;

    struct io_stats {
        uint64_t ops = 0;
        uint64_t bytes = 0;
        std::chrono::duration<double> duration = std::chrono::duration<double>(0);

        void update(uint64_t len, std::chrono::duration<double> lat) {
            ops++;
            bytes += len;
            duration += lat;
        }
    };

    struct group_client {
        http::experimental::client http;
        io_stats read_stats;
        io_stats write_stats;
        seastar::metrics::metric_groups metrics;
        group_client(std::unique_ptr<http::experimental::connection_factory> f, unsigned max_conn);
        void register_metrics(std::string class_name, std::string host);
    };

    std::unordered_map<seastar::scheduling_group, group_client> _https;
    using global_factory = std::function<shared_ptr<client>(std::string)>;
    global_factory _gf;
    semaphore& _memory;

    struct private_tag {};

    future<semaphore_units<>> claim_memory(size_t mem);

    void authorize(http::request&);
    group_client& find_or_create_client();
    future<> make_request(http::request req, http::experimental::client::reply_handler handle = ignore_reply, http::reply::status_type expected = http::reply::status_type::ok);

    using reply_handler_ext = noncopyable_function<future<>(group_client&, const http::reply&, input_stream<char>&& body)>;
    future<> make_request(http::request req, reply_handler_ext handle, http::reply::status_type expected = http::reply::status_type::ok);

public:
    explicit client(std::string host, endpoint_config_ptr cfg, semaphore& mem, global_factory gf, private_tag);
    static shared_ptr<client> make(std::string endpoint, endpoint_config_ptr cfg, semaphore& memory, global_factory gf = {});

    future<> operate(temporary_buffer<char> buf, opcode_type op_code);

    void update_config(endpoint_config_ptr);

    struct handle {
        std::string _host;
        global_factory _gf;
    public:
        handle(const client& cln)
                : _host(cln._host)
                , _gf(cln._gf)
        {}

        shared_ptr<client> to_client() && {
            return _gf(std::move(_host));
        }
    };

    future<> close();
};

} // dynamodb namespace
