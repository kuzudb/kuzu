#include "include/node_prepared_statement.h"

Napi::Object NodePreparedStatement::Init(Napi::Env env, Napi::Object exports) {
    Napi::HandleScope scope(env);

    Napi::Function t = DefineClass(env, "NodePreparedStatement",
        {
            InstanceMethod("initAsync", &NodePreparedStatement::InitAsync),
            InstanceMethod("initSync", &NodePreparedStatement::InitSync),
            InstanceMethod("isSuccess", &NodePreparedStatement::IsSuccess),
            InstanceMethod("getErrorMessage", &NodePreparedStatement::GetErrorMessage),
        });

    exports.Set("NodePreparedStatement", t);
    return exports;
}

NodePreparedStatement::NodePreparedStatement(const Napi::CallbackInfo& info)
    : Napi::ObjectWrap<NodePreparedStatement>(info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);
    NodeConnection* nodeConnection =
        Napi::ObjectWrap<NodeConnection>::Unwrap(info[0].As<Napi::Object>());
    connection = nodeConnection->connection;
    queryString = info[1].ToString();
}

Napi::Value NodePreparedStatement::InitAsync(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);
    auto callback = info[0].As<Napi::Function>();
    auto* asyncWorker = new PreparedStatementInitAsyncWorker(callback, this);
    asyncWorker->Queue();
    return info.Env().Undefined();
}

Napi::Value NodePreparedStatement::InitSync(const Napi::CallbackInfo& info) {
    InitCppPreparedStatement();
    return info.Env().Undefined();
}

void NodePreparedStatement::InitCppPreparedStatement() {
    preparedStatement = connection->prepare(queryString);
    connection.reset();
}

Napi::Value NodePreparedStatement::IsSuccess(const Napi::CallbackInfo& info) {
    if (preparedStatement == nullptr) {
        return Napi::Boolean::New(info.Env(), false);
    }
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);
    return Napi::Boolean::New(env, preparedStatement->isSuccess());
}

Napi::Value NodePreparedStatement::GetErrorMessage(const Napi::CallbackInfo& info) {
    if (preparedStatement == nullptr) {
        return Napi::String::New(info.Env(), "Prepared statement is not initialized.");
    }
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);
    return Napi::String::New(env, preparedStatement->getErrorMessage());
}
