#pragma once

#include "restinio/all.hpp"

#include "src/runner/include/server/session.h"

namespace graphflow {
namespace runner {

using router_t = restinio::router::express_router_t<>;
using request_handle_t = router_t::actual_request_handle_t;

class Server {

public:
    void run(const string& host, int port);

private:
    void createErrorResponse(request_handle_t req, exception& ex);

    // endpoint: [PUT] .../bufferPoolSize
    void registerPUTBufferPoolSize(unique_ptr<router_t>& router);

    // endpoint: [GET] .../bufferPoolSize
    void registerGETBufferPoolSize(unique_ptr<router_t>& router);

    // endpoint: [POST] .../load
    void registerPOSTLoad(unique_ptr<router_t>& router);

    // endpoint: [GET] .../prettyPlan
    void registerGETPrettyPlan(unique_ptr<router_t>& router);

    // endpoint: [POST] .../execute
    void registerPOSTExecute(unique_ptr<router_t>& router);

private:
    Session session;
};

} // namespace runner
} // namespace graphflow