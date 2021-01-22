#pragma once

#include "nlohmann/json.hpp"
#include "restinio/all.hpp"

#include "src/runner/include/server/session.h"

namespace graphflow {
namespace runner {

using router_t = restinio::router::express_router_t<>;
using request_handle_t = router_t::actual_request_handle_t;

class Server {

public:
    Server();

    void run(const string& host, int port);

private:
    void createErrorResponse(request_handle_t req, exception& ex);

    // endpoint: [GET] .../help
    void registerGETHelp(unique_ptr<router_t>& router);

    // endpoint: [PUT] .../bufferPoolSize
    void registerPUTBufferPoolSize(unique_ptr<router_t>& router);

    // endpoint: [PUT] .../numProcessorThreads
    void registerPUTNumProcessorThreads(unique_ptr<router_t>& router);

    // endpoint: [GET] .../session
    void registerGETSession(unique_ptr<router_t>& router);

    // endpoint: [POST] .../load
    void registerPOSTLoad(unique_ptr<router_t>& router);

    // endpoint: [GET] .../prettyPlan
    void registerGETPrettyPlan(unique_ptr<router_t>& router);

    // endpoint: [POST] .../execute
    void registerPOSTExecute(unique_ptr<router_t>& router);

    // endpoint: [GET] .../graphDebugInfo
    void registerGETGraphDebugInfo(unique_ptr<router_t>& router);

private:
    Session session;
    nlohmann::json helpCatalog;
};

} // namespace runner
} // namespace graphflow
