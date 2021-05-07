#pragma once

#include "nlohmann/json.hpp"
#include "restinio/all.hpp"

#include "src/main/include/session.h"
#include "src/main/include/system.h"

namespace graphflow {
namespace main {

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

    // System-level functions

    // endpoint: [PUT] .../bufferPoolSize
    void registerPUTBufferPoolSize(unique_ptr<router_t>& router);

    // endpoint: [PUT] .../numProcessorThreads
    void registerPUTNumProcessorThreads(unique_ptr<router_t>& router);

    // endpoint: [POST] .../load
    void registerPOSTLoad(unique_ptr<router_t>& router);

    // Session-level functions

    // endpoint: [POST] .../execute
    // At present, we assume autoCommit = true.
    void registerPOSTExecute(unique_ptr<router_t>& router);

    // endpoint: [GET] .../graphDebugInfo
    void registerGETDebugInfo(unique_ptr<router_t>& router);

private:
    unique_ptr<System> system;
    SystemConfig config;

    // At present, the server runs on a single thread and supports only one session.
    Session session;

    nlohmann::json helpCatalog;
};

} // namespace main
} // namespace graphflow
