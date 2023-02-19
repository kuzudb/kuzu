#include "node_query_result.h"
#include "AllAsyncWorker.h"
#include "EachAsyncWorker.h"

#include "main/kuzu.h"

using namespace kuzu::main;

Napi::FunctionReference NodeQueryResult::constructor;

Napi::Object NodeQueryResult::Wrap(Napi::Env env, shared_ptr<kuzu::main::QueryResult>& queryResult) {
    Napi::HandleScope scope(env);
    Napi::Object obj = DefineClass(env, "NodeQueryResult", {
      InstanceMethod("hasNext", &NodeQueryResult::HasNext),
      InstanceMethod("getNext", &NodeQueryResult::GetNext),
      InstanceMethod("close", &NodeQueryResult::Close),
      InstanceMethod("all", &NodeQueryResult::All),
      InstanceMethod("each", &NodeQueryResult::Each),
   }).New({});
    NodeQueryResult * nodeQueryResult = Unwrap(obj);
    nodeQueryResult->queryResult = queryResult;
    return obj;
}

NodeQueryResult::NodeQueryResult(const Napi::CallbackInfo& info) : Napi::ObjectWrap<NodeQueryResult>(info)  {}

Napi::Value NodeQueryResult::HasNext(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);

    if (info.Length()!=0) {
        Napi::TypeError::New(env, "hasNext takes no arguments").ThrowAsJavaScriptException();
        return Napi::Object::New(env);
    }

    return Napi::Boolean::New(env, this->queryResult->hasNext());
}

Napi::Value NodeQueryResult::GetNext(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);

    if (info.Length()!=0) {
        Napi::TypeError::New(env, "getNext takes no arguments").ThrowAsJavaScriptException();
        return Napi::Object::New(env);
    }

    auto row = this->queryResult->getNext();
    Napi::Array obj = Napi::Array::New(env, row->len());
    for (int j=0; j < row->len(); j++) {
        obj.Set(j, row->getValue(j)->toString());
    }
   return obj;
}

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

    AllAsyncWorker * asyncWorker = new AllAsyncWorker(callback, queryResult);
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

    EachAsyncWorker * asyncWorker = new EachAsyncWorker(callback, queryResult);
    asyncWorker->Queue();
    return info.Env().Undefined();
}

NodeQueryResult::~NodeQueryResult() {}
