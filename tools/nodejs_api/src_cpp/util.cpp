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
    case kuzu::common::DATE: {
        auto dateVal = value.getValue<date_t>();
        int32_t year, month, day;
        kuzu::common::Date::Convert(dateVal, year, month, day);
        return Napi::Date::New(env, GetEpochFromDate(year, month, day));
    }
    default:
        Napi::TypeError::New(env, "Unsupported type: " + kuzu::common::Types::dataTypeToString(dataType));
    }
    return Napi::Value();
}

unsigned long Util::GetEpochFromDate(int32_t year, int32_t month, int32_t day) {
    std::tm t{};
    t.tm_year = year - 1900;
    t.tm_mon = month - 1;
    t.tm_mday = day;
    return std::chrono::system_clock::from_time_t(std::mktime(&t)).time_since_epoch().count();
}
