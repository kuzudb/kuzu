#include "include/util.h"

Napi::Value Util::ConvertToNapiObject(const kuzu::common::Value& value, Napi::Env env) {
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
        Napi::TypeError::New(env, "Unsupported type: " + kuzu::common::Types::dataTypeToString(dataType));
    }
    return Napi::Value();
}
