#include "include/tsfn_context.h"
#include "include/util.h"

// The thread entry point. This takes as its arguments the specific
// threadsafe-function context created inside the main thread.
void TsfnContext::threadEntry(TsfnContext* context) {
    // This callback transforms the native addon data (int *data) to JavaScript
    // values. It also receives the treadsafe-function's registered callback, and
    // may choose to call it.
    auto callback = [](Napi::Env env, Napi::Function jsCallback, TsfnContext * context) {
        Napi::Array arr = Napi::Array::New(env);
        size_t i = 0;
        auto columnNames = context->queryResult->getColumnNames();
        Napi::Array colArray = Napi::Array::New(env);
        for (auto i = 0u; i < columnNames.size(); ++i) {
            colArray.Set(i, columnNames[i]);
        }
        if (context->type == TsfnContext::EACH) { jsCallback.Call({env.Null(), colArray}); }
        else { arr.Set(i++, colArray); }
        while (context->queryResult->hasNext()) {
            auto row = context->queryResult->getNext();
            Napi::Array rowArray = Napi::Array::New(env);
            for (size_t j = 0; j < row->len(); j++) {
                Napi::Value val = Util::ConvertToNapiObject(*row->getValue(j), env);
                rowArray.Set(j, val);
            }
            if (context->type == TsfnContext::EACH) { jsCallback.Call({env.Null(), rowArray}); }
            else { arr.Set(i++, rowArray); }
        }
        if (context->type == TsfnContext::ALL) { jsCallback.Call({env.Null(), arr}); }
        else if (context->type == TsfnContext::EACH) { context->_doneCallback->Call({}); }
    };

    auto errorCallback = []( Napi::Env env, Napi::Function jsCallback ) {
        Napi::Error error = Napi::Error::New(env, "Unsuccessful async all callback");
        jsCallback.Call({error.Value(), env.Undefined()});
    };

    // Perform a call into JavaScript.
    napi_status status =
        context->tsfn.BlockingCall(context, callback);

    if (status != napi_ok) {
        Napi::Error::Fatal(
            "ThreadEntry",
            "Napi::ThreadSafeNapi::Function.BlockingCall() failed");
        context->tsfn.BlockingCall( errorCallback );
    }

    // Release the thread-safe function. This decrements the internal thread
    // count, and will perform finalization since the count will reach 0.
    context->tsfn.Release();
}

void TsfnContext::FinalizerCallback(Napi::Env env, void* finalizeData, TsfnContext* context) {
    // Join the thread
    context->nativeThread.join();

    // Resolve the Promise previously returned to JS via the CreateTSFN method.
    context->deferred.Resolve(Napi::Boolean::New(env, true));
    delete context;
}

