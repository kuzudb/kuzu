#pragma once

#include <ctime>
#include <iostream>
#include <chrono>

#include <napi.h>
#include "main/kuzu.h"

class Util {
    public:
        static Napi::Value ConvertToNapiObject(const kuzu::common::Value& value, Napi::Env env);
        static unsigned long GetEpochFromDate(int32_t year, int32_t month, int32_t day);
        static unordered_map<string, shared_ptr<kuzu::common::Value>> transformParameters(Napi::Array params);
        static kuzu::common::Value transformNapiValue(Napi::Value val);

        // Data structure representing our thread-safe function context.
        struct TsfnContext {
            TsfnContext(Napi::Env env, shared_ptr<kuzu::main::QueryResult> queryResult) :
                  deferred(Napi::Promise::Deferred::New(env)),
                  queryResult(queryResult) {};

            // Native Promise returned to JavaScript
            Napi::Promise::Deferred deferred;

            shared_ptr<kuzu::main::QueryResult> queryResult;

            // Native thread
            std::thread nativeThread;

            Napi::ThreadSafeFunction tsfn;
        };

        // The thread entry point. This takes as its arguments the specific
        // threadsafe-function context created inside the main thread.
        static void threadEntry(TsfnContext* context);

        // The thread-safe function finalizer callback. This callback executes
        // at destruction of thread-safe function, taking as arguments the finalizer
        // data and threadsafe-function context.
        static void FinalizerCallback(Napi::Env env, void* finalizeData, TsfnContext* context);
};
