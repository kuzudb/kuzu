#include "node_query_result.h"

#include "all_each_async_worker.h"
#include "each_single_async_worker.h"

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
        AllEachAsyncWorker* asyncWorker = new AllEachAsyncWorker(callback, queryResult, "all");
        asyncWorker->Queue();
    } catch(const std::exception &exc) {
        Napi::TypeError::New(env, "Unsuccessful all callback: " + std::string(exc.what())).ThrowAsJavaScriptException();
    }
    return info.Env().Undefined();
}

Napi::Value NodeQueryResult::Each(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);

    if (info.Length()!=2 || !info[0].IsFunction() || !info[1].IsFunction()) {
        Napi::TypeError::New(env, "Each needs a callback").ThrowAsJavaScriptException();
        return Napi::Object::New(env);
    }

    Function eachCallback = info[0].As<Function>();
    Function doneCallback = info[1].As<Function>();

    try {
//        AllEachAsyncWorker* asyncWorker = new AllEachAsyncWorker(doneCallback, queryResult, "each", eachCallback);
//        asyncWorker->Queue();
        size_t i = 0;
        while (this->queryResult->hasNext()) {
            auto row = this->queryResult->getNext();
            EachSingleAsyncWorker * asyncWorker = new EachSingleAsyncWorker(eachCallback, row, i);
            asyncWorker->Queue();
            i++;
        }
    } catch(const std::exception &exc) {
        Napi::TypeError::New(env, "Unsuccessful each callback: " + std::string(exc.what())).ThrowAsJavaScriptException();
    }
    return info.Env().Undefined();
}

NodeQueryResult::~NodeQueryResult() {}
