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

unordered_map<string, shared_ptr<kuzu::common::Value>> Util::transformParameters(Napi::Array params) {
    unordered_map<string, shared_ptr<kuzu::common::Value>> result;
    for (size_t i = 0; i < params.Length(); i++){
        Napi::Array param = params.Get(i).As<Napi::Array>();
        if (param.Length()!=2) {
            throw runtime_error("Each parameter must be in the form of <name, val>");
        } else if (!param.Get(uint32_t(0)).IsString()) {
            throw runtime_error("Parameter name must be of type string but get " + param.Get(uint32_t(0)).Type());
        }
        string name = param.Get(uint32_t(0)).ToString();
        auto transformedVal = transformNapiValue(param.Get(uint32_t(1)));
        result.insert({name, make_shared<kuzu::common::Value>(transformedVal)});
    }
    return result;
}

kuzu::common::Value Util::transformNapiValue(Napi::Value val) {
    if (val.IsBoolean()) {
        bool temp = val.ToBoolean();
        return Value::createValue<bool>(temp);
    } else if (val.IsNumber()) {
        double temp = val.ToNumber().DoubleValue();
        if (floor(temp) == temp) {
            return Value::createValue<int64_t>(temp);
        } else {
            return Value::createValue<double_t>(temp);
        }
    } else if (val.IsString()) {
        string temp = val.ToString();
        return Value::createValue<string>(temp);
    } else if (val.IsDate()) {
        double dateMilliseconds = val.As<Napi::Date>().ValueOf();
        time_t seconds = (time_t)(dateMilliseconds/1000);
        auto tm = gmtime(&seconds);
        auto year = tm->tm_year + 1900;
        auto month = tm->tm_mon + 1;
        auto day = tm->tm_mday;
        auto hour = tm->tm_hour;
        auto minute = tm->tm_min;
        auto second = tm->tm_sec;
        auto date = Date::FromDate(year, month, day);
        auto time = Time::FromTime(hour, minute, second);
        return Value::createValue<timestamp_t>(Timestamp::FromDatetime(date, time));
    } else {
        throw runtime_error("Unknown parameter type " + val.Type());
    }
}
