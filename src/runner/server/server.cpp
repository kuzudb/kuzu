#include "src/runner/include/server/server.h"

namespace graphflow {
namespace runner {

void Server::run(const string& host, int port) {
    auto router = make_unique<router_t>();
    registerPUTBufferPoolSize(router);
    registerGETBufferPoolSize(router);
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
        .set_body("{error : \"" + string(ex.what()) + "\"}")
        .done();
    return;
}

void Server::registerPUTBufferPoolSize(unique_ptr<router_t>& router) {
    router->http_put("/bufferPoolSize", [&](request_handle_t req, auto) {
        try {
            auto size = stoll(req->body());
            session.setBufferPoolSize(size);
            req->create_response()
                .append_header(
                    restinio::http_field::content_type, "application/json; charset=utf-8")
                .set_body("{msg : \"BufferPool size has been set to " + to_string(size) + " B\"}")
                .done();
        } catch (exception& ex) { createErrorResponse(req, ex); }
        return restinio::request_accepted();
    });
}

void Server::registerGETBufferPoolSize(unique_ptr<router_t>& router) {
    router->http_get("/bufferPoolSize", [&](request_handle_t req, auto) {
        req->create_response()
            .append_header(restinio::http_field::content_type, "application/json; charset=utf-8")
            .set_body("{msg : \"" + to_string(session.getBufferPoolSize()) + " B\"}")
            .done();
        return restinio::request_accepted();
    });
}

void Server::registerPOSTLoad(unique_ptr<router_t>& router) {
    router->http_post("/load", [&](request_handle_t req, auto) {
        try {
            auto path = req->body();
            session.loadGraph(path);
            req->create_response()
                .append_header(
                    restinio::http_field::content_type, "application/json; charset=utf-8")
                .set_body("{msg : \"Intialized the Graph at " + path + "\"}")
                .done();
        } catch (exception& ex) { createErrorResponse(req, ex); }
        return restinio::request_accepted();
    });
}

void Server::registerGETPrettyPlan(unique_ptr<router_t>& router) {
    router->http_get("/prettyPlan", [&](request_handle_t req, auto) {
        try {
            auto plan = session.getPrettyPlan(req->body());
            req->create_response()
                .append_header(
                    restinio::http_field::content_type, "application/json; charset=utf-8")
                .set_body(plan->dump())
                .done();
        } catch (exception& ex) { createErrorResponse(req, ex); }
        return restinio::request_accepted();
    });
}

void Server::registerPOSTExecute(unique_ptr<router_t>& router) {
    router->http_post("/execute", [&](request_handle_t req, auto) {
        try {
            auto plan = session.getExecutionStats(req->body());
            req->create_response()
                .append_header(
                    restinio::http_field::content_type, "application/json; charset=utf-8")
                .set_body(plan->dump())
                .done();
        } catch (exception& ex) { createErrorResponse(req, ex); }
        return restinio::request_accepted();
    });
}

} // namespace runner
} // namespace graphflow