#pragma once

#include <optional>
#include <seastar/core/shared_ptr.hh>

namespace dynamodb {

struct endpoint_config {
    unsigned port = 3833;
    bool use_https = false;

    struct aws_config {
        // the access key of the credentials
        std::string access_key_id;
        // the secret key of the credentials
        std::string secret_access_key;
        // the security token, only for session credentials
        std::string session_token;
        std::string region;
    };

    std::optional<aws_config> aws;
};

using endpoint_config_ptr = seastar::lw_shared_ptr<endpoint_config>;

} // dynamodb namespace
