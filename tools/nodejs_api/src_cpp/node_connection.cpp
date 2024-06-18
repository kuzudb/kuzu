#include "include/node_connection.h"

#include <iostream>

#include "include/node_database.h"
#include "include/node_query_result.h"
#include "include/node_util.h"
#include "main/kuzu.h"

Napi::Object NodeConnection::Init(Napi::Env env, Napi::Object exports) {
    Napi::HandleScope scope(env);

    Napi::Function t = DefineClass(env, "NodeConnection",
        {InstanceMethod("initAsync", &NodeConnection::InitAsync),
            InstanceMethod("executeAsync", &NodeConnection::ExecuteAsync),
            InstanceMethod("queryAsync", &NodeConnection::QueryAsync),
            InstanceMethod("setMaxNumThreadForExec", &NodeConnection::SetMaxNumThreadForExec),
            InstanceMethod("setQueryTimeout", &NodeConnection::SetQueryTimeout),
            InstanceMethod("close", &NodeConnection::Close)});

    exports.Set("NodeConnection", t);
    return exports;
}

NodeConnection::NodeConnection(const Napi::CallbackInfo& info)
    : Napi::ObjectWrap<NodeConnection>(info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);
    NodeDatabase* nodeDatabase = Napi::ObjectWrap<NodeDatabase>::Unwrap(info[0].As<Napi::Object>());
    database = nodeDatabase->database;
}

Napi::Value NodeConnection::InitAsync(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);
    auto callback = info[0].As<Napi::Function>();
    auto* asyncWorker = new ConnectionInitAsyncWorker(callback, this);
    asyncWorker->Queue();
    return info.Env().Undefined();
}

void NodeConnection::InitCppConnection() {
    this->connection = std::make_shared<Connection>(database.get());
    connection->getClientContext()->getProgressBar()->setDisplay(
        std::make_shared<NodeProgressBarDisplay>());
    // After the connection is initialized, we do not need to hold a reference to the database.
    database.reset();
}

void NodeConnection::SetMaxNumThreadForExec(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);
    size_t numThreads = info[0].ToNumber().Int64Value();
    try {
        this->connection->setMaxNumThreadForExec(numThreads);
    } catch (const std::exception& exc) {
        Napi::Error::New(env, std::string(exc.what())).ThrowAsJavaScriptException();
    }
}

void NodeConnection::SetQueryTimeout(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);
    size_t timeout = info[0].ToNumber().Int64Value();
    try {
        this->connection->setQueryTimeOut(timeout);
    } catch (const std::exception& exc) {
        Napi::Error::New(env, std::string(exc.what())).ThrowAsJavaScriptException();
    }
}

void NodeConnection::Close(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);
    this->connection.reset();
}

Napi::Value NodeConnection::ExecuteAsync(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);
    auto nodePreparedStatement =
        Napi::ObjectWrap<NodePreparedStatement>::Unwrap(info[0].As<Napi::Object>());
    auto nodeQueryResult = Napi::ObjectWrap<NodeQueryResult>::Unwrap(info[1].As<Napi::Object>());
    auto callback = info[3].As<Napi::Function>();
    try {
        auto params = Util::TransformParametersForExec(info[2].As<Napi::Array>());
        auto asyncWorker = new ConnectionExecuteAsyncWorker(callback, connection,
            nodePreparedStatement->preparedStatement, nodeQueryResult, std::move(params), info[4]);
        asyncWorker->Queue();
    } catch (const std::exception& exc) {
        Napi::Error::New(env, std::string(exc.what())).ThrowAsJavaScriptException();
    }
    return info.Env().Undefined();
}

Napi::Value NodeConnection::QueryAsync(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);
    auto statement = info[0].As<Napi::String>().Utf8Value();
    auto nodeQueryResult = Napi::ObjectWrap<NodeQueryResult>::Unwrap(info[1].As<Napi::Object>());
    auto callback = info[2].As<Napi::Function>();
    auto asyncWorker =
        new ConnectionQueryAsyncWorker(callback, connection, statement, nodeQueryResult, info[3]);
    asyncWorker->Queue();
    return info.Env().Undefined();
}
