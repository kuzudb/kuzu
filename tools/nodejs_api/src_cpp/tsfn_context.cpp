#include "include/tsfn_context.h"
#include "include/util.h"

void TsfnContext::threadEntry(TsfnContext* context) {
    auto callback = [](Napi::Env env, Napi::Function jsCallback, TsfnContext * context) {
        Napi::Array arr = Napi::Array::New(env);
        size_t i = 0;
        auto columnNames = context->queryResult->getColumnNames();
        while (context->queryResult->hasNext()) {
            auto row = context->queryResult->getNext();
            Napi::Object rowArray = Napi::Object::New(env);
            for (size_t j = 0; j < row->len(); j++) {
                Napi::Value val = Util::ConvertToNapiObject(*row->getValue(j), env);
                rowArray.Set( (j >= columnNames.size()) ? "" : columnNames[j], val);
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

    napi_status status =
        context->tsfn.BlockingCall(context, callback);

    if (status != napi_ok) {
        Napi::Error::Fatal(
            "ThreadEntry",
            "Napi::ThreadSafeNapi::Function.BlockingCall() failed");
        context->tsfn.BlockingCall( errorCallback );
    }

    context->tsfn.Release();
}

void TsfnContext::FinalizerCallback(Napi::Env env, void* finalizeData, TsfnContext* context) {
    context->nativeThread.join();

    context->deferred.Resolve(Napi::Boolean::New(env, true));
    delete context;
}

