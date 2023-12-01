#include "include/node_query_result.h"

#include <thread>

#include "include/node_util.h"
#include "main/kuzu.h"

using namespace kuzu::main;

Napi::Object NodeQueryResult::Init(Napi::Env env, Napi::Object exports) {
    Napi::HandleScope scope(env);

    Napi::Function t = DefineClass(env, "NodeQueryResult",
        {
            InstanceMethod("resetIterator", &NodeQueryResult::ResetIterator),
            InstanceMethod("hasNext", &NodeQueryResult::HasNext),
            InstanceMethod("getNumTuples", &NodeQueryResult::GetNumTuples),
            InstanceMethod("getNextAsync", &NodeQueryResult::GetNextAsync),
            InstanceMethod("getColumnDataTypesAsync", &NodeQueryResult::GetColumnDataTypesAsync),
            InstanceMethod("getColumnNamesAsync", &NodeQueryResult::GetColumnNamesAsync),
        });

    exports.Set("NodeQueryResult", t);
    return exports;
}

NodeQueryResult::NodeQueryResult(const Napi::CallbackInfo& info)
    : Napi::ObjectWrap<NodeQueryResult>(info) {}

void NodeQueryResult::SetQueryResult(std::shared_ptr<kuzu::main::QueryResult>& queryResult) {
    this->queryResult = queryResult;
}

void NodeQueryResult::ResetIterator(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);
    try {
        this->queryResult->resetIterator();
    } catch (const std::exception& exc) {
        Napi::Error::New(env, std::string(exc.what())).ThrowAsJavaScriptException();
    }
}

Napi::Value NodeQueryResult::HasNext(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);
    try {
        return Napi::Boolean::New(env, this->queryResult->hasNext());
    } catch (const std::exception& exc) {
        Napi::Error::New(env, std::string(exc.what())).ThrowAsJavaScriptException();
    }
    return info.Env().Undefined();
}

Napi::Value NodeQueryResult::GetNumTuples(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);
    try {
        return Napi::Number::New(env, this->queryResult->getNumTuples());
    } catch (const std::exception& exc) {
        Napi::Error::New(env, std::string(exc.what())).ThrowAsJavaScriptException();
    }
    return info.Env().Undefined();
}

Napi::Value NodeQueryResult::GetNextAsync(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);
    auto callback = info[0].As<Napi::Function>();
    auto* asyncWorker = new NodeQueryResultGetNextAsyncWorker(callback, this);
    asyncWorker->Queue();
    return info.Env().Undefined();
}

Napi::Value NodeQueryResult::GetColumnDataTypesAsync(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);
    auto callback = info[0].As<Napi::Function>();
    auto* asyncWorker = new NodeQueryResultGetColumnMetadataAsyncWorker(
        callback, this, GetColumnMetadataType::DATA_TYPE);
    asyncWorker->Queue();
    return info.Env().Undefined();
}

Napi::Value NodeQueryResult::GetColumnNamesAsync(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);
    auto callback = info[0].As<Napi::Function>();
    auto* asyncWorker = new NodeQueryResultGetColumnMetadataAsyncWorker(
        callback, this, GetColumnMetadataType::NAME);
    asyncWorker->Queue();
    return info.Env().Undefined();
}
