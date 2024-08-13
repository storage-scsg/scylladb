#include <initializer_list>
#include <memory>
#include <stdexcept>
#include <rapidxml.h>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <boost/range/adaptor/map.hpp>
#include <seastar/core/coroutine.hh>
#include <seastar/core/gate.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/util/short_streams.hh>
#include <seastar/http/request.hh>
#include "utils/dynamodb/client.hh"
#include "utils/http.hh"
#include "utils/memory_data_sink.hh"
#include "utils/chunked_vector.hh"
#include "utils/aws_sigv4.hh"
#include "db_clock.hh"
#include "log.hh"

namespace dynamodb {

static logging::logger dl("dynamodb");

// desperated
future<> ignore_reply(const http::reply& rep, input_stream<char>&& in_) {
    auto in = std::move(in_);
    co_await util::skip_entire_stream(in);
}

future<sstring> parse_reply(const http::reply& rep, input_stream<char>&& in_) {
    auto in = std::move(in_);
    co_return co_await util::read_entire_stream_contiguous(in);
}

client::client(std::string host, endpoint_config_ptr cfg, semaphore& mem, global_factory gf, private_tag)
        : _host(std::move(host))
        , _cfg(std::move(cfg))
        , _gf(std::move(gf))
        , _memory(mem)
{
}

void client::update_config(endpoint_config_ptr cfg) {
    if (_cfg->port != cfg->port || _cfg->use_https != cfg->use_https) {
        throw std::runtime_error("Updating port and/or https usage is not possible");
    }
    _cfg = std::move(cfg);
}

shared_ptr<client> client::make(std::string endpoint, endpoint_config_ptr cfg, semaphore& mem, global_factory gf) {
    return seastar::make_shared<client>(std::move(endpoint), std::move(cfg), mem, std::move(gf), private_tag{});
}

void client::authorize(http::request& req) {
    // if (!_cfg->aws) {
    //     return;
    // }
    auto time_point_str = utils::aws::format_time_point(db_clock::now());
    auto time_point_st = time_point_str.substr(0, 8);
    req._headers["x-amz-date"] = time_point_str;
    req._headers["x-amz-content-sha256"] = "UNSIGNED-PAYLOAD";

    if (!_cfg->aws->session_token.empty()) {
        req._headers["x-amz-security-token"] = _cfg->aws->session_token;
    }

    std::map<std::string_view, std::string_view> signed_headers;
    sstring signed_headers_list = "";
    // AWS requires all x-... and Host: headers to be signed
    signed_headers["host"] = req._headers["Host"];

    for (const auto& [name, value] : req._headers) {
        if (name.starts_with("x-")) {
            signed_headers[name] = value;
        }
    }

    unsigned header_nr = signed_headers.size();
    for (const auto& h : signed_headers) {
        signed_headers_list += format("{}{}", h.first, header_nr == 1 ? "" : ";");
        header_nr--;
    }

    sstring query_string = "";
    std::map<std::string_view, std::string_view> query_parameters;
    for (const auto& q : req.query_parameters) {
        query_parameters[q.first] = q.second;
    }
    unsigned query_nr = query_parameters.size();
    for (const auto& q : query_parameters) {
        query_string += format("{}={}{}", q.first, q.second, query_nr == 1 ? "" : "&");
        query_nr--;
    }

    auto sig = utils::aws::get_signature(
        _cfg->aws->access_key_id, _cfg->aws->secret_access_key,
        _host, req._url, req._method,
        utils::aws::omit_datestamp_expiration_check,
        signed_headers_list, signed_headers,
        utils::aws::unsigned_content,
        _cfg->aws->region, "dynamodb", query_string);
    req._headers["Authorization"] = format("AWS4-HMAC-SHA256 Credential={}/{}/{}/dynamodb/aws4_request,SignedHeaders={},Signature={}", _cfg->aws->access_key_id, time_point_st, _cfg->aws->region, signed_headers_list, sig);
}

future<semaphore_units<>> client::claim_memory(size_t size) {
    return get_units(_memory, size);
}

client::group_client::group_client(std::unique_ptr<http::experimental::connection_factory> f, unsigned max_conn)
        : http(std::move(f), max_conn)
{
}

// class_name: schedule_group_name
// host: ip+port (ip:port)
void client::group_client::register_metrics(std::string class_name, std::string host) {
    namespace sm = seastar::metrics;
    auto ep_label = sm::label("endpoint")(host);
    auto sg_label = sm::label("class")(class_name);
    metrics.add_group("dynamodb", {
        sm::make_gauge("nr_connections", [this] { return http.connections_nr(); },
                sm::description("Total number of connections"), {ep_label, sg_label}),
        sm::make_gauge("nr_active_connections", [this] { return http.connections_nr() - http.idle_connections_nr(); },
                sm::description("Total number of connections with running requests"), {ep_label, sg_label}),
        sm::make_counter("total_new_connections", [this] { return http.total_new_connections_nr(); },
                sm::description("Total number of new connections created so far"), {ep_label, sg_label}),
        sm::make_counter("total_read_requests", [this] { return read_stats.ops; },
                sm::description("Total number of object read requests"), {ep_label, sg_label}),
        sm::make_counter("total_write_requests", [this] { return write_stats.ops; },
                sm::description("Total number of object write requests"), {ep_label, sg_label}),
        sm::make_counter("total_read_bytes", [this] { return read_stats.bytes; },
                sm::description("Total number of bytes read from objects"), {ep_label, sg_label}),
        sm::make_counter("total_write_bytes", [this] { return write_stats.bytes; },
                sm::description("Total number of bytes written to objects"), {ep_label, sg_label}),
        sm::make_counter("total_read_latency_sec", [this] { return read_stats.duration.count(); },
                sm::description("Total time spent reading data from objects"), {ep_label, sg_label}),
        sm::make_counter("total_write_latency_sec", [this] { return write_stats.duration.count(); },
                sm::description("Total time spend writing data to objects"), {ep_label, sg_label}),
    });
}

client::group_client& client::find_or_create_client() {
    auto sg = current_scheduling_group();
    auto it = _https.find(sg);
    if (it == _https.end()) [[unlikely]] {
        auto factory = std::make_unique<utils::http::dns_connection_factory>(_host, _cfg->port, _cfg->use_https, dl);
        // Limit the maximum number of connections this group's http client
        // may have proportional to its shares. Shares are typically in the
        // range of 100...1000, thus resulting in 1..10 connections
        auto max_connections = _cfg->max_connections;
        if (max_connections == 0) {
            max_connections = std::max((unsigned)(sg.get_shares() / 100), 1u);
        }
        it = _https.emplace(std::piecewise_construct,
            std::forward_as_tuple(sg),
            std::forward_as_tuple(std::move(factory), max_connections)
        ).first;

        std::string endpoint = seastar::format("{}:{}", _host, _cfg->port);
        it->second.register_metrics(sg.name(), std::move(endpoint));
    }
    return it->second;
}

future<sstring> client::make_request(http::request req, http::experimental::client::reply_handler_with_string handle, http::reply::status_type expected) {
    authorize(req);
    auto& gc = find_or_create_client();
    return gc.http.make_request(std::move(req), std::move(handle), expected);
}

future<sstring> client::make_request(http::request req, reply_handler_ext handle_ex, http::reply::status_type expected) {
    authorize(req);
    auto& gc = find_or_create_client();
    auto handle = [&gc, handle = std::move(handle_ex)] (const http::reply& rep, input_stream<char>&& in) {
        return handle(gc, rep, std::move(in));
    };

    return gc.http.make_request(std::move(req), std::move(handle), expected);
}

// return the response message or an exception future with error
future<sstring> client::operate(temporary_buffer<char> buf, opcode_type op_code)
{
    auto req = http::request::make("POST", _host, "/");
    std::string headers = format("DynamoDB_20120810.{}", opcode_map[op_code]);
    req._headers["x-amz-target"] = std::move(headers);
    auto len = buf.size();

    req.write_body(/*content_type=*/ "json", len, [buf = std::move(buf)] (output_stream<char>&& out_) mutable -> future<> {
        auto out = std::move(out_);
        std::exception_ptr ex;
        try {
            co_await out.write(buf.get(), buf.size());
            co_await out.flush();
        } catch (...) {
            ex = std::current_exception();
        }
        co_await out.close();

        if (ex) {
            co_await coroutine::return_exception_ptr(std::move(ex));
        }
    });

    co_return co_await make_request(std::move(req), [len, start = dynamodb_clock::now()] (group_client& gc, const auto& rep, auto&& in) {
        gc.write_stats.update(len, dynamodb_clock::now() - start);
        return parse_reply(rep, std::move(in));
    });
}

future<> client::close() {
    co_await coroutine::parallel_for_each(_https, [] (auto& it) -> future<> {
        co_await it.second.http.close();
    });
}

} // dynamodb namespace
