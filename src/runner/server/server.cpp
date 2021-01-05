#include "src/runner/include/server/server.h"

namespace graphflow {
namespace runner {

Server::Server() {
    helpCatalog["help"] = nlohmann::json::array();
}

void Server::run(const string& host, int port) {
    auto router = make_unique<router_t>();
    registerGETHelp(router);
    registerPUTBufferPoolSize(router);
    registerPUTNumProcessorThreads(router);
    registerGETSession(router);
    registerPOSTLoad(router);
    registerGETPrettyPlan(router);
    registerPOSTExecute(router);

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
            nlohmann::json json;
            json["msg"] = "BufferPool size has been set to " +
                          to_string(session.setBufferPoolSize(size)) + " B";
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
            nlohmann::json json;
            json["msg"] = "Number of processor Threads has been set to " +
                          to_string(session.setNumProcessorThreads(num)) + ".";
            req->create_response()
                .append_header(
                    restinio::http_field::content_type, "application/json; charset=utf-8")
                .set_body(json.dump())
                .done();
        } catch (exception& ex) { createErrorResponse(req, ex); }
        return restinio::request_accepted();
    });
}

void Server::registerGETSession(unique_ptr<router_t>& router) {
    nlohmann::json json;
    json["method"] = "GET";
    json["url"] = "/session";
    json["desc"] = "Gets the current state of session.";
    helpCatalog["help"].push_back(json);

    router->http_get("/session", [&](request_handle_t req, auto) {
        req->create_response()
            .append_header(restinio::http_field::content_type, "application/json; charset=utf-8")
            .set_body(session.getSession()->dump())
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
            session.loadGraph(path);
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

void Server::registerGETPrettyPlan(unique_ptr<router_t>& router) {
    nlohmann::json json;
    json["method"] = "GET";
    json["url"] = "/prettyPlan";
    json["dataParams"] = "<path_to_serialized_query_plan>";
    json["desc"] = "Print the query plan in a human-readable format.";
    helpCatalog["help"].push_back(json);

    router->http_get("/prettyPlan", [&](request_handle_t req, auto) {
        try {
            req->create_response()
                .append_header(
                    restinio::http_field::content_type, "application/json; charset=utf-8")
                .set_body(session.getPrettyPlan(req->body())->dump())
                .done();
        } catch (exception& ex) { createErrorResponse(req, ex); }
        return restinio::request_accepted();
    });
}

void Server::registerPOSTExecute(unique_ptr<router_t>& router) {
    nlohmann::json json;
    json["method"] = "POST";
    json["url"] = "/execute";
    json["dataParams"] = "<path_to_serialized_query_plan>";
    json["desc"] = "Execute a query plan.";
    json["note"] = "Executing the query is synchronous at present.";
    helpCatalog["help"].push_back(json);

    router->http_post("/execute", [&](request_handle_t req, auto) {
        try {
            stringstream stream(req->body().data());
            string plan_path;
            uint32_t numThreads;
            stream >> plan_path;
            stream >> numThreads;
            req->create_response()
                .append_header(
                    restinio::http_field::content_type, "application/json; charset=utf-8")
                .set_body(session.execute(plan_path, numThreads)->dump())
                .done();
        } catch (exception& ex) { createErrorResponse(req, ex); }
        return restinio::request_accepted();
    });
}

} // namespace runner
} // namespace graphflow
