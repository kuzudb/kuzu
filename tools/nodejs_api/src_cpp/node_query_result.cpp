#include "include/node_query_result.h"

#include "include/all_async_worker.h"
#include "include/each_async_worker.h"
#include "main/kuzu.h"
#include "include/util.h"
#include <thread>
#include <array>
#include <type_traits>

using namespace kuzu::main;

std::thread nativeThread;
Napi::ThreadSafeFunction tsfn;
typedef kuzu::common::Value * valuePtr;

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

Napi::Value NodeQueryResult::All(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);

    if (info.Length()!=1 || !info[0].IsFunction()) {
        Napi::TypeError::New(env, "All needs a callback").ThrowAsJavaScriptException();
        return Napi::Object::New(env);
    }

    Function callback = info[0].As<Function>();

    tsfn = Napi::ThreadSafeFunction::New(
        env,
        callback,  // JavaScript function called asynchronously
        "Resource Name",         // Name
        0,                       // Unlimited queue
        1,                       // Only one thread will use this initially
        []( Napi::Env ) {        // Finalizer used to clean threads up
            nativeThread.join();
        } );
    shared_ptr<kuzu::main::QueryResult> localQueryResult = queryResult;

    nativeThread = std::thread( [localQueryResult] {
        auto callback = []( Napi::Env env, Function jsCallback, valuePtr ** allResult ) {
            size_t numRows = 0;
            for(size_t i = 0;;i++){
                if (allResult[i] == nullptr) break;
                numRows++;
            }

            size_t numCols = 0;
            if (numRows > 0) {
                for (size_t j = 0;; j++) {
                    if (allResult[0][j] == nullptr)
                        break;
                    numCols++;
                }
            }

            Napi::Array arr = Napi::Array::New(env, numRows);
            for (size_t i = 0; i < numRows; i++) {
                Napi::Array rowArray = Napi::Array::New(env, numCols);
                size_t j = 0;
                for (; j < numCols; j++) {
                    rowArray.Set(j, Util::ConvertToNapiObject(*allResult[i][j], env));
                    delete allResult[i][j];
                }
                delete[] allResult[i];
                arr.Set(i, rowArray);
            }
            delete allResult;
            jsCallback.Call({env.Null(), arr});
        };

        auto errorCallback = []( Napi::Env env, Function jsCallback ) {
            Napi::Error error = Napi::Error::New(env, "Unsuccessful async all callback");
            jsCallback.Call({error.Value(), env.Undefined()});
        };

        vector<vector<valuePtr>> allResult;
        size_t i = 0;
        while (localQueryResult->hasNext()) {
            auto row = localQueryResult->getNext();
            allResult.emplace_back(vector<valuePtr>(row->len()));
            for (size_t j = 0; j < row->len(); j++) {
                allResult[i][j] = new kuzu::common::Value(*row->getValue(j));
            }
            i++;
        }

        const int numRows = allResult.size();
        size_t tempCols = 0;
        if (numRows > 0) tempCols = allResult[0].size();
        const int numCols = tempCols;

        auto valueArray = new valuePtr * [numRows + 1];
        for (i = 0; i < numRows; i++) {
            valueArray[i] = new valuePtr[numCols + 1];
            size_t j = 0;
            for (; j < numCols; j++){
                valueArray[i][j] = allResult[i][j];
            }
            valueArray[i][j] = nullptr;
        }
        valueArray[i] = nullptr;

        napi_status status = tsfn.BlockingCall( valueArray, callback );
        if ( status != napi_ok )
        {
            tsfn.BlockingCall( errorCallback );
        }

        tsfn.Release();
    } );

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
