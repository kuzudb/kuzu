#include "include/node_query_result.h"

#include "include/all_async_worker.h"
#include "include/each_single_async_worker.h"
#include "main/kuzu.h"

using namespace kuzu::main;

Napi::FunctionReference NodeQueryResult::constructor;

Napi::Object NodeQueryResult::Init(Napi::Env env, Napi::Object exports) {
    Napi::HandleScope scope(env);

    Napi::Function t = DefineClass(env, "NodeQueryResult", {
       InstanceMethod("close", &NodeQueryResult::Close),
       InstanceMethod("all", &NodeQueryResult::All),
       InstanceMethod("getNext", &NodeQueryResult::GetNext),
       InstanceMethod("hasNext", &NodeQueryResult::HasNext),
    });

    constructor = Napi::Persistent(t);
    constructor.SuppressDestruct();

    exports.Set("NodeQueryResult", t);
    return exports;
}

NodeQueryResult::NodeQueryResult(const Napi::CallbackInfo& info) : Napi::ObjectWrap<NodeQueryResult>(info)  {}

void NodeQueryResult::SetQueryResult(shared_ptr<kuzu::main::QueryResult> & inputQueryResult) {
    this->queryResult = inputQueryResult;
}

void NodeQueryResult::Close(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);

    try {
        this->queryResult.reset();
    } catch(const std::exception &exc) {
        Napi::TypeError::New(env, "Unsuccessful queryResult close: " + std::string(exc.what())).ThrowAsJavaScriptException();
    }

}

Napi::Value NodeQueryResult::All(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);

    if (info.Length()!=1 || !info[0].IsFunction()) {
        Napi::TypeError::New(env, "All needs a callback").ThrowAsJavaScriptException();
        return Napi::Object::New(env);
    }

    Function callback = info[0].As<Function>();
    try {
        AllAsyncWorker* asyncWorker = new AllAsyncWorker(callback, queryResult);
        asyncWorker->Queue();
    } catch(const std::exception &exc) {
        Napi::TypeError::New(env, "Unsuccessful all callback: " + std::string(exc.what())).ThrowAsJavaScriptException();
    }
    return info.Env().Undefined();
}

Napi::Value NodeQueryResult::GetNext(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);

    if (info.Length()!=1 || !info[0].IsFunction()) {
        Napi::TypeError::New(env, "Each needs a callback").ThrowAsJavaScriptException();
        return Napi::Object::New(env);
    }

    Function eachCallback = info[0].As<Function>();

    if (!queryResult->hasNext()){
        Napi::TypeError::New(env, "Cannot call getNext queryResult is empty").ThrowAsJavaScriptException();
        return Napi::Object::New(env);
    }

    try {
        EachSingleAsyncWorker * asyncWorker = new EachSingleAsyncWorker(eachCallback, queryResult, 1);
        asyncWorker->Queue();
    } catch(const std::exception &exc) {
        Napi::TypeError::New(env, "Unsuccessful getNext callback: " + std::string(exc.what())).ThrowAsJavaScriptException();
    }
    return info.Env().Undefined();
}

Napi::Value NodeQueryResult::HasNext(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);

    if (info.Length()!=0) {
        Napi::TypeError::New(env, "hasNext takes no arguments").ThrowAsJavaScriptException();
        return Napi::Object::New(env);
    }

    try {
        return Napi::Boolean::New(env, queryResult->hasNext());
    } catch(const std::exception &exc) {
        Napi::TypeError::New(env, "Unsuccessful hasNext: " + std::string(exc.what())).ThrowAsJavaScriptException();
    }
    return info.Env().Undefined();
}

NodeQueryResult::~NodeQueryResult() {}
