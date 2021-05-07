#include "src/main/include/server.h"

namespace graphflow {
namespace main {

Server::Server() : session{system} {
    helpCatalog["help"] = nlohmann::json::array();
}

void Server::run(const string& host, int port) {
    auto router = make_unique<router_t>();
    registerGETHelp(router);
    registerPOSTLoad(router);
    registerPUTBufferPoolSize(router);
    registerPUTNumProcessorThreads(router);
    registerPOSTExecute(router);
    registerGETDebugInfo(router);

    try {
        using traits_t = restinio::traits_t<restinio::asio_timer_manager_t,
            restinio::single_threaded_ostream_logger_t, router_t>;
        restinio::run(restinio::on_this_thread<traits_t>().port(port).address(host).request_handler(
            move(router)));
    } catch (exception& ex) { cout << "Error: " << ex.what() << endl; }
}

void Server::createErrorResponse(request_handle_t req, exception& ex) {
    req->create_response(restinio::status_bad_request())
        .append_header(restinio::http_field::content_type, "application/json; charset=utf-8")
        .set_body("{\"error\" : \"" + string(ex.what()) + "\"}")
        .done();
    return;
}

void Server::registerGETHelp(unique_ptr<router_t>& router) {
    nlohmann::json json;
    json["method"] = "GET";
    json["url"] = "/help";
    json["desc"] = "Returns a list of all API endpoints with description and usage.";
    helpCatalog["help"].push_back(json);

    router->http_get("/help", [&](request_handle_t req, auto) {
        req->create_response()
            .append_header(restinio::http_field::content_type, "application/json; charset=utf-8")
            .set_body(helpCatalog.dump())
            .done();
        return restinio::request_accepted();
    });
}

void Server::registerPOSTLoad(unique_ptr<router_t>& router) {
    nlohmann::json json;
    json["method"] = "POST";
    json["url"] = "/load";
    json["dataParams"] = "<path_to_serialized_data>";
    json["desc"] = "Loads the serialized graph data from the path.";
    helpCatalog["help"].push_back(json);

    router->http_post("/load", [&](request_handle_t req, auto) {
        try {
            auto path = req->body();
            system.reset(new System(config, path));
            nlohmann::json json;
            json["msg"] = "Intialized the Graph at " + path + ".";
            req->create_response()
                .append_header(
                    restinio::http_field::content_type, "application/json; charset=utf-8")
                .set_body(json.dump())
                .done();
        } catch (exception& ex) { createErrorResponse(req, ex); }
        return restinio::request_accepted();
    });
}

void Server::registerPUTBufferPoolSize(unique_ptr<router_t>& router) {
    nlohmann::json json;
    json["method"] = "PUT";
    json["url"] = "/bufferPoolSize";
    json["dataParams"] = "<size>";
    json["desc"] = "Changes the bufferPool size.";
    json["notes"] = {"Size of the bufferPool should be in Bytes.",
        "Size should be in multiple of 4096 (system PAGE_SIZE).", "Bounds on size are 1MB and 1TB.",
        "The previously loaded Graph is dropped on successful change of bufferSize."};
    helpCatalog["help"].push_back(json);

    router->http_put("/bufferPoolSize", [&](request_handle_t req, auto) {
        try {
            auto size = stoll(req->body());
            config.setBufferPoolSize(size);
            if (nullptr != system.get()) {
                system->restart(config);
            }
            nlohmann::json json;
            json["msg"] = "BufferPool size has been set to " + to_string(size) + " B";
            req->create_response()
                .append_header(
                    restinio::http_field::content_type, "application/json; charset=utf-8")
                .set_body(json.dump())
                .done();
        } catch (exception& ex) { createErrorResponse(req, ex); }
        return restinio::request_accepted();
    });
}

void Server::registerPUTNumProcessorThreads(unique_ptr<router_t>& router) {
    nlohmann::json json;
    json["method"] = "PUT";
    json["url"] = "/numProcessorThreads";
    json["dataParams"] = "<num>";
    json["desc"] = "Changes the number of Processor threads.";
    json["notes"] = {"Number should be greater than 0 and less than the number of physical threads "
                     "available in the system.",
        "Processor is restarted, bufferPool is not affected."};
    helpCatalog["help"].push_back(json);

    router->http_put("/numProcessorThreads", [&](request_handle_t req, auto) {
        try {
            auto num = stoi(req->body());
            config.setNumProcessorThreads(num);
            if (nullptr != system.get()) {
                system->restart(config);
            }
            nlohmann::json json;
            json["msg"] = "Number of processor Threads has been set to " + to_string(num) + ".";
            req->create_response()
                .append_header(
                    restinio::http_field::content_type, "application/json; charset=utf-8")
                .set_body(json.dump())
                .done();
        } catch (exception& ex) { createErrorResponse(req, ex); }
        return restinio::request_accepted();
    });
}

void Server::registerPOSTExecute(unique_ptr<router_t>& router) {
    nlohmann::json json;
    json["method"] = "POST";
    json["url"] = "/execute";
    json["dataParams"] = "<cypher_query>";
    json["desc"] = "Execute a query.";
    json["note"] = "Executing the query is synchronous at present.";
    helpCatalog["help"].push_back(json);

    router->http_post("/execute", [&](request_handle_t req, auto) {
        try {
            stringstream stream(req->body().data());
            string cypherQuery;
            uint32_t numThreads;
            stream >> cypherQuery;
            stream >> numThreads;
            auto result = session.submitQuery(cypherQuery, numThreads);
            req->create_response()
                .append_header(
                    restinio::http_field::content_type, "application/json; charset=utf-8")
                .set_body(result->dump())
                .done();
        } catch (exception& ex) { createErrorResponse(req, ex); }
        return restinio::request_accepted();
    });
}

void Server::registerGETDebugInfo(unique_ptr<router_t>& router) {
    nlohmann::json json;
    json["method"] = "GET";
    json["url"] = "/graphDebugInfo";
    json["desc"] = "Gets general information about the System and current Session.";
    helpCatalog["help"].push_back(json);
    router->http_get("/graphDebugInfo", [&](request_handle_t req, auto) {
        req->create_response()
            .append_header(restinio::http_field::content_type, "text/plain; charset=utf-8")
            .set_body(session.debugInfo()->dump())
            .done();

        return restinio::request_accepted();
    });
}

} // namespace main
} // namespace graphflow
