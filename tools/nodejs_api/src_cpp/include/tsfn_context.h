#pragma once

#include <chrono>

#include <napi.h>
#include "main/kuzu.h"
#include <iostream>

using namespace std;

// Data structure representing our thread-safe function context.
struct TsfnContext {
    enum Type { ALL, EACH };

    TsfnContext(Napi::Env env, shared_ptr<kuzu::main::QueryResult> queryResult, Type type) :
          deferred(Napi::Promise::Deferred::New(env)),
          queryResult(queryResult),
          type(type) {};

    TsfnContext(Napi::Env env, shared_ptr<kuzu::main::QueryResult> queryResult, Type type,
        Napi::Function doneCallback) :
          deferred(Napi::Promise::Deferred::New(env)),
          queryResult(queryResult),
          type(type) {
        _doneCallback = make_unique<Napi::FunctionReference>(Napi::FunctionReference(Napi::Persistent(doneCallback)));
        _doneCallback->SuppressDestruct();
    };

    Type type;

    // Native Promise returned to JavaScript
    Napi::Promise::Deferred deferred;

    shared_ptr<kuzu::main::QueryResult> queryResult;

    // Native thread
    std::thread nativeThread;

    Napi::ThreadSafeFunction tsfn;

    unique_ptr<Napi::FunctionReference> _doneCallback;

    // The thread entry point. This takes as its arguments the specific
    // threadsafe-function context created inside the main thread.
    static void threadEntry(TsfnContext* context);

    // The thread-safe function finalizer callback. This callback executes
    // at destruction of thread-safe function, taking as arguments the finalizer
    // data and threadsafe-function context.
    static void FinalizerCallback(Napi::Env env, void* finalizeData, TsfnContext* context);
};

