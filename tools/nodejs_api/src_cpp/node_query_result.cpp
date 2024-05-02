#include "include/node_query_result.h"

#include <thread>

#include "include/node_util.h"
#include "main/kuzu.h"

using namespace kuzu::main;

Napi::Object NodeQueryResult::Init(Napi::Env env, Napi::Object exports) {
    Napi::HandleScope scope(env);

    Napi::Function t = DefineClass(env, "NodeQueryResult",
        {InstanceMethod("resetIterator", &NodeQueryResult::ResetIterator),
            InstanceMethod("hasNext", &NodeQueryResult::HasNext),
            InstanceMethod("hasNextQueryResult", &NodeQueryResult::HasNextQueryResult),
            InstanceMethod("getNextQueryResultAsync", &NodeQueryResult::GetNextQueryResultAsync),
            InstanceMethod("getNumTuples", &NodeQueryResult::GetNumTuples),
            InstanceMethod("getNextAsync", &NodeQueryResult::GetNextAsync),
            InstanceMethod("getColumnDataTypesAsync", &NodeQueryResult::GetColumnDataTypesAsync),
            InstanceMethod("getColumnNamesAsync", &NodeQueryResult::GetColumnNamesAsync),
            InstanceMethod("close", &NodeQueryResult::Close)});

    exports.Set("NodeQueryResult", t);
    return exports;
}

NodeQueryResult::NodeQueryResult(const Napi::CallbackInfo& info)
    : Napi::ObjectWrap<NodeQueryResult>(info) {}

NodeQueryResult::~NodeQueryResult() {
    this->Close();
}

void NodeQueryResult::SetQueryResult(QueryResult* queryResult, bool isOwned) {
    this->queryResult = queryResult;
    this->isOwned = isOwned;
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

Napi::Value NodeQueryResult::HasNextQueryResult(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);
    try {
        return Napi::Boolean::New(env, this->queryResult->hasNextQueryResult());
    } catch (const std::exception& exc) {
        Napi::Error::New(env, std::string(exc.what())).ThrowAsJavaScriptException();
    }
    return info.Env().Undefined();
}

Napi::Value NodeQueryResult::GetNextQueryResultAsync(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);
    auto newQueryResult = Napi::ObjectWrap<NodeQueryResult>::Unwrap(info[0].As<Napi::Object>());
    auto callback = info[1].As<Napi::Function>();
    auto* asyncWorker =
        new NodeQueryResultGetNextQueryResultAsyncWorker(callback, this, newQueryResult);
    asyncWorker->Queue();
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
    auto* asyncWorker = new NodeQueryResultGetColumnMetadataAsyncWorker(callback, this,
        GetColumnMetadataType::DATA_TYPE);
    asyncWorker->Queue();
    return info.Env().Undefined();
}

Napi::Value NodeQueryResult::GetColumnNamesAsync(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);
    auto callback = info[0].As<Napi::Function>();
    auto* asyncWorker = new NodeQueryResultGetColumnMetadataAsyncWorker(callback, this,
        GetColumnMetadataType::NAME);
    asyncWorker->Queue();
    return info.Env().Undefined();
}

void NodeQueryResult::Close(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);
    this->Close();
}

void NodeQueryResult::Close() {
    if (this->isOwned) {
        delete this->queryResult;
        this->queryResult = nullptr;
    }
}
