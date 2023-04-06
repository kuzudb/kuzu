#include "include/node_query_result.h"

#include "main/kuzu.h"
#include "include/util.h"
#include "include/tsfn_context.h"
#include <thread>
#include <chrono>

using namespace kuzu::main;

Napi::Object NodeQueryResult::Init(Napi::Env env, Napi::Object exports) {
    Napi::HandleScope scope(env);

    Napi::Function t = DefineClass(env, "NodeQueryResult", {
       InstanceMethod("close", &NodeQueryResult::Close),
       InstanceMethod("all", &NodeQueryResult::All),
       InstanceMethod("each", &NodeQueryResult::Each),
       InstanceMethod("getColumnDataTypes", &NodeQueryResult::GetColumnDataTypes),
       InstanceMethod("getColumnNames", &NodeQueryResult::GetColumnNames),
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
        Napi::Error::New(env, "Unsuccessful queryResult close: " + std::string(exc.what())).ThrowAsJavaScriptException();
    }

}

// Exported JavaScript function. Creates the thread-safe function and native
// thread. Promise is resolved in the thread-safe function's finalizer.
Napi::Value NodeQueryResult::All(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);

    // Construct context data
    auto testData = new TsfnContext(env, queryResult, TsfnContext::ALL);

    // Create a new ThreadSafeFunction.
    testData->tsfn = Napi::ThreadSafeFunction::New(
        env,                           // Environment
        info[0].As<Napi::Function>(),  // JS function from caller
        "TSFN",                        // Resource name
        0,                             // Max queue size (0 = unlimited).
        1,                             // Initial thread count
        testData,                      // Context,
        TsfnContext::FinalizerCallback,// Finalizer
        (void*)nullptr                 // Finalizer data
    );
    testData->nativeThread = std::thread(TsfnContext::threadEntry, testData);

    // Return the deferred's Promise. This Promise is resolved in the thread-safe
    // function's finalizer callback.
    return testData->deferred.Promise();
}


// Exported JavaScript function. Creates the thread-safe function and native
// thread. Promise is resolved in the thread-safe function's finalizer.
Napi::Value NodeQueryResult::Each(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);

    // Construct context data
    auto testData = new TsfnContext(env, queryResult, TsfnContext::TsfnContext::EACH, info[1].As<Napi::Function>());

    // Create a new ThreadSafeFunction.
    testData->tsfn = Napi::ThreadSafeFunction::New(
        env,                           // Environment
        info[0].As<Napi::Function>(),  // JS function from caller
        "TSFN",                        // Resource name
        0,                             // Max queue size (0 = unlimited).
        1,                             // Initial thread count
        testData,                      // Context,
        TsfnContext::FinalizerCallback,// Finalizer
        (void * ) nullptr  // Finalizer data
    );
    testData->nativeThread = std::thread(TsfnContext::threadEntry, testData);

    // Return the deferred's Promise. This Promise is resolved in the thread-safe
    // function's finalizer callback.
    return testData->deferred.Promise();
}

Napi::Value NodeQueryResult::GetColumnDataTypes(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);

    auto columnDataTypes = queryResult->getColumnDataTypes();
    Napi::Array arr = Napi::Array::New(env, columnDataTypes.size());

    for (auto i = 0u; i < columnDataTypes.size(); ++i) {
        arr.Set(i, kuzu::common::Types::dataTypeToString(columnDataTypes[i]));
    }
    return arr;
}

Napi::Value NodeQueryResult::GetColumnNames(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);

    auto columnNames = queryResult->getColumnNames();
    Napi::Array arr = Napi::Array::New(env, columnNames.size());

    for (auto i = 0u; i < columnNames.size(); ++i) {
        arr.Set(i, columnNames[i]);
    }
    return arr;
}