#include "node_query_result.h"

#include "all_each_async_worker.h"
#include "main/kuzu.h"

using namespace kuzu::main;

Napi::FunctionReference NodeQueryResult::constructor;

Napi::Object NodeQueryResult::Wrap(Napi::Env env, shared_ptr<kuzu::main::QueryResult>& queryResult) {
    Napi::HandleScope scope(env);
    Napi::Object obj = DefineClass(env, "NodeQueryResult", {
      InstanceMethod("close", &NodeQueryResult::Close),
      InstanceMethod("all", &NodeQueryResult::All),
      InstanceMethod("each", &NodeQueryResult::Each),
   }).New({});
    NodeQueryResult * nodeQueryResult = Unwrap(obj);
    nodeQueryResult->queryResult = queryResult;
    return obj;
}

NodeQueryResult::NodeQueryResult(const Napi::CallbackInfo& info) : Napi::ObjectWrap<NodeQueryResult>(info)  {}

void NodeQueryResult::Close(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);

    this->queryResult.reset();
}

Napi::Value NodeQueryResult::All(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);

    if (info.Length()!=1 || !info[0].IsFunction()) {
        Napi::TypeError::New(env, "All needs a callback").ThrowAsJavaScriptException();
        return Napi::Object::New(env);
    }

    Function callback = info[0].As<Function>();

    AllEachAsyncWorker * asyncWorker = new AllEachAsyncWorker(callback, queryResult, "all");
    asyncWorker->Queue();
    return info.Env().Undefined();
}

Napi::Value NodeQueryResult::Each(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);

    if (info.Length()!=1 || !info[0].IsFunction()) {
        Napi::TypeError::New(env, "Each needs a callback").ThrowAsJavaScriptException();
        return Napi::Object::New(env);
    }

    Function callback = info[0].As<Function>();

    AllEachAsyncWorker * asyncWorker = new AllEachAsyncWorker(callback, queryResult, "each");
    asyncWorker->Queue();
    return info.Env().Undefined();
}

NodeQueryResult::~NodeQueryResult() {}
