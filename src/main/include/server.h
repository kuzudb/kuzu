#pragma once

#include "nlohmann/json.hpp"
#include "restinio/all.hpp"

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
    void createErrorResponse(request_handle_t& req, exception& ex);

    // endpoint: [GET] .../help
    void registerGETHelp(router_t& router);

    // endpoint: [POST] .../load
    // this function create the system and establish one session
    void registerPOSTLoad(router_t& router);

    // endpoint: [POST] .../execute
    // At present, we assume autoCommit = true.
    void registerPOSTExecute(router_t& router);

    // endpoint: [GET] .../graphDebugInfo
    void registerGETDebugInfo(router_t& router);

    nlohmann::json executeQuery(SessionContext& sessionContext);

private:
    unique_ptr<System> system;
    // At present, the server runs on a single thread and supports only one session.
    nlohmann::json helpCatalog;
};

} // namespace main
} // namespace graphflow
