#include "include/node_connection.h"

#include <iostream>

#include "include/node_database.h"
#include "include/node_query_result.h"
#include "include/node_util.h"
#include "main/kuzu.h"

Napi::Object NodeConnection::Init(Napi::Env env, Napi::Object exports) {
    Napi::HandleScope scope(env);

    Napi::Function t = DefineClass(env, "NodeConnection",
        {
            InstanceMethod("initAsync", &NodeConnection::InitAsync),
            InstanceMethod("executeAsync", &NodeConnection::ExecuteAsync),
            InstanceMethod("setMaxNumThreadForExec", &NodeConnection::SetMaxNumThreadForExec),
            InstanceMethod("getNodeTableNamesAsync", &NodeConnection::GetNodeTableNamesAsync),
            InstanceMethod("getRelTableNamesAsync", &NodeConnection::GetRelTableNamesAsync),
            InstanceMethod("getNodePropertyNamesAsync", &NodeConnection::GetNodePropertyNamesAsync),
            InstanceMethod("getRelPropertyNamesAsync", &NodeConnection::GetRelPropertyNamesAsync),
        });

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
    // After the connection is initialized, we do not need to hold a reference to the database.
    database.reset();
}

Napi::Value NodeConnection::ExecuteAsync(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);
    auto nodePreparedStatement =
        Napi::ObjectWrap<NodePreparedStatement>::Unwrap(info[0].As<Napi::Object>());
    auto nodeQueryResult = Napi::ObjectWrap<NodeQueryResult>::Unwrap(info[1].As<Napi::Object>());
    auto callback = info[3].As<Napi::Function>();
    try {
        auto parameterMap = nodePreparedStatement->preparedStatement->getParameterMap();
        auto params = Util::TransformParametersForExec(info[2].As<Napi::Array>(), parameterMap);
        auto asyncWorker = new ConnectionExecuteAsyncWorker(callback, connection,
            nodePreparedStatement->preparedStatement, nodeQueryResult, params);
        asyncWorker->Queue();
    } catch (const std::exception& exc) {
        Napi::Error::New(env, std::string(exc.what())).ThrowAsJavaScriptException();
    }
    return info.Env().Undefined();
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

Napi::Value NodeConnection::GetNodeTableNamesAsync(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);
    auto callback = info[0].As<Napi::Function>();
    auto* asyncWorker = new ConnectionTableMetadataAsyncWorker(callback, this, "", NODE_TABLE_NAME);
    asyncWorker->Queue();
    return info.Env().Undefined();
}

Napi::Value NodeConnection::GetRelTableNamesAsync(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);
    auto callback = info[0].As<Napi::Function>();
    auto* asyncWorker = new ConnectionTableMetadataAsyncWorker(callback, this, "", REL_TABLE_NAME);
    asyncWorker->Queue();
    return info.Env().Undefined();
}

Napi::Value NodeConnection::GetNodePropertyNamesAsync(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);
    std::string tableName = info[0].ToString();
    auto callback = info[1].As<Napi::Function>();
    auto* asyncWorker =
        new ConnectionTableMetadataAsyncWorker(callback, this, tableName, NODE_PROPERTY_NAME);
    asyncWorker->Queue();
    return info.Env().Undefined();
}

Napi::Value NodeConnection::GetRelPropertyNamesAsync(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);
    std::string tableName = info[0].ToString();
    auto callback = info[1].As<Napi::Function>();
    auto* asyncWorker =
        new ConnectionTableMetadataAsyncWorker(callback, this, tableName, REL_PROPERTY_NAME);
    asyncWorker->Queue();
    return info.Env().Undefined();
}
