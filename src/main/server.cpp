#include "src/main/include/server.h"

#include "src/common/include/utils.h"

namespace graphflow {
namespace main {

Server::Server() {
    helpCatalog["help"] = nlohmann::json::array();
}

void Server::run(const string& host, int port) {
    auto router = make_unique<router_t>();
    registerGETHelp(*router);
    registerPOSTLoad(*router);
    registerPOSTExecute(*router);
    registerGETDebugInfo(*router);

    try {
        using traits_t = restinio::traits_t<restinio::asio_timer_manager_t,
            restinio::single_threaded_ostream_logger_t, router_t>;
        restinio::run(restinio::on_this_thread<traits_t>().port(port).address(host).request_handler(
            move(router)));
    } catch (exception& ex) { cout << "Error: " << ex.what() << endl; }
}

void Server::createErrorResponse(request_handle_t& req, exception& ex) {
    req->create_response(restinio::status_bad_request())
        .append_header(restinio::http_field::content_type, "application/json; charset=utf-8")
        .set_body("{\"error\" : \"" + string(ex.what()) + "\"}")
        .done();
}

void Server::registerGETHelp(router_t& router) {
    nlohmann::json json;
    json["method"] = "GET";
    json["url"] = "/help";
    json["desc"] = "Returns a list of all API endpoints with description and usage.";
    helpCatalog["help"].push_back(json);

    router.http_get("/help", [&](request_handle_t req, auto) {
        req->create_response()
            .append_header(restinio::http_field::content_type, "application/json; charset=utf-8")
            .set_body(helpCatalog.dump())
            .done();
        return restinio::request_accepted();
    });
}

/**
 * Create a system (database instance) by loading serialized dataset.
 * Start a session.
 */
void Server::registerPOSTLoad(router_t& router) {
    nlohmann::json json;
    json["method"] = "POST";
    json["url"] = "/load";
    json["dataParams"] = "<path_to_serialized_data>";
    json["desc"] = "Loads the serialized graph data from the path.";
    helpCatalog["help"].push_back(json);

    router.http_post("/load", [&](request_handle_t req, auto) {
        try {
            auto path = req->body();
            system = make_unique<System>(path);
            session = make_unique<Session>(*system);
            nlohmann::json json;
            json["msg"] = "Initialized the Graph at " + path + ".";
            req->create_response()
                .append_header(
                    restinio::http_field::content_type, "application/json; charset=utf-8")
                .set_body(json.dump())
                .done();
        } catch (exception& ex) { createErrorResponse(req, ex); }
        return restinio::request_accepted();
    });
}

/**
 * Submit a query and its execution parameters
 */
void Server::registerPOSTExecute(router_t& router) {
    nlohmann::json json;
    json["method"] = "POST";
    json["url"] = "/execute";
    json["dataParams"] = "<cypher_query>";
    json["desc"] = "Execute a query.";
    json["note"] = "Executing the query is synchronous at present.";
    helpCatalog["help"].push_back(json);

    router.http_post("/execute", [&](request_handle_t req, auto) {
        try {
            auto input = StringUtils::split(req->body(), "|");
            session->sessionContext->query = input[0];
            if (input.size() > 1) {
                assert(input.size() == 2);
                session->sessionContext->numThreads = stoul(input[1]);
            }
            auto result = session->executeQuery();
            req->create_response()
                .append_header(
                    restinio::http_field::content_type, "application/json; charset=utf-8")
                .set_body(result.dump())
                .done();
        } catch (exception& ex) { createErrorResponse(req, ex); }
        return restinio::request_accepted();
    });
}

void Server::registerGETDebugInfo(router_t& router) {
    nlohmann::json json;
    json["method"] = "GET";
    json["url"] = "/graphDebugInfo";
    json["desc"] = "Gets general information about the System and current Session.";
    helpCatalog["help"].push_back(json);
    router.http_get("/graphDebugInfo", [&](request_handle_t req, auto) {
        req->create_response()
            .append_header(restinio::http_field::content_type, "text/plain; charset=utf-8")
            .set_body(session->debugInfo().dump())
            .done();
        return restinio::request_accepted();
    });
}

} // namespace main
} // namespace graphflow
