#include "include/node_query_result.h"

#include "include/all_async_worker.h"
#include "include/each_async_worker.h"
#include "main/kuzu.h"
#include "include/util.h"
#include <thread>
#include <chrono>

using namespace kuzu::main;

std::thread nativeThread;
Napi::ThreadSafeFunction tsfn;

Napi::FunctionReference NodeQueryResult::constructor;

Napi::Object NodeQueryResult::Init(Napi::Env env, Napi::Object exports) {
    Napi::HandleScope scope(env);

    Napi::Function t = DefineClass(env, "NodeQueryResult", {
       InstanceMethod("close", &NodeQueryResult::Close),
       InstanceMethod("all", &NodeQueryResult::All),
       InstanceMethod("getNext", &NodeQueryResult::GetNext),
       InstanceMethod("hasNext", &NodeQueryResult::HasNext),
    });

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

// Exported JavaScript function. Creates the thread-safe function and native
// thread. Promise is resolved in the thread-safe function's finalizer.
Napi::Value NodeQueryResult::All(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);

    if (info.Length()!=1 || !info[0].IsFunction()) {
        Napi::TypeError::New(env, "All needs a callback").ThrowAsJavaScriptException();
        return Napi::Object::New(env);
    }

    // Construct context data
    auto testData = new Util::TsfnContext(env, queryResult);

    // Create a new ThreadSafeFunction.
    testData->tsfn = Napi::ThreadSafeFunction::New(
        env,                           // Environment
        info[0].As<Napi::Function>(),  // JS function from caller
        "TSFN",                        // Resource name
        0,                             // Max queue size (0 = unlimited).
        1,                             // Initial thread count
        testData,                      // Context,
        Util::FinalizerCallback,       // Finalizer
        (void*)nullptr                 // Finalizer data
    );
    testData->nativeThread = std::thread(Util::threadEntry, testData);

    // Return the deferred's Promise. This Promise is resolved in the thread-safe
    // function's finalizer callback.
    return testData->deferred.Promise();
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
        EachAsyncWorker * asyncWorker = new EachAsyncWorker(eachCallback, queryResult, 1);
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
