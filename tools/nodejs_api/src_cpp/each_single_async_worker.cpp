#include "each_single_async_worker.h"

#include <chrono>
#include <thread>
#include <vector>

#include "node_query_result.h"

using namespace std;

EachSingleAsyncWorker::EachSingleAsyncWorker(Function& callback, shared_ptr<kuzu::common::FlatTuple>& flatTuple, size_t index)
    : AsyncWorker(callback), flatTuple(flatTuple), index(index) {};

void EachSingleAsyncWorker::Execute() {
    try {
        for (size_t j = 0; j < flatTuple->len(); j++) {
            rowResult.emplace_back(
                make_unique<kuzu::common::Value>(kuzu::common::Value(*flatTuple->getValue(j))));
        }
    } catch(const std::exception &exc) {
        SetError("Unsuccessful async Each callback: " + std::string(exc.what()));
    }
}

void EachSingleAsyncWorker::OnOK() {
    Napi::Array rowArray = Napi::Array::New(Env(), rowResult.size() + 1);
    size_t j = 0;
    for (; j < rowResult.size(); j++){
        rowArray.Set(j, ConvertToNapiObject(*rowResult[j], Env()));
    }
    rowArray.Set(j, Napi::Number::New(Env(), index));
    Callback().Call({Env().Null(), rowArray});
}


Napi::Value EachSingleAsyncWorker::ConvertToNapiObject(const kuzu::common::Value& value, Napi::Env env) {
    if (value.isNull()) {
        return Napi::Value();
    }
    auto dataType = value.getDataType();
    switch (dataType.typeID) {
    case kuzu::common::BOOL: {
        return Napi::Boolean::New(env, value.getValue<bool>());
    }
    case kuzu::common::INT64: {
        return Napi::Number::New(env, value.getValue<int64_t>());
    }
    case kuzu::common::DOUBLE: {
        return Napi::Number::New(env, value.getValue<double>());
    }
    case kuzu::common::STRING: {
        return Napi::String::New(env, value.getValue<string>());
    }
    case kuzu::common::LIST: {
        auto& listVal = value.getListValReference();
        Napi::Array arr = Napi::Array::New(env, listVal.size());
        for (auto i = 0u; i < listVal.size(); ++i) {
            arr.Set(i, ConvertToNapiObject(*listVal[i], env));
        }
        return arr;
    }
    default:
        SetError("Unsupported type: " + kuzu::common::Types::dataTypeToString(dataType));
    }
    return Napi::Value();
}
