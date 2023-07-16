#include "include/node_util.h"

#include "common/exception.h"

Napi::Value Util::ConvertToNapiObject(const Value& value, Napi::Env env) {
    if (value.isNull()) {
        return env.Null();
    }
    auto dataType = value.getDataType();
    switch (dataType.getLogicalTypeID()) {
    case LogicalTypeID::BOOL: {
        return Napi::Boolean::New(env, value.getValue<bool>());
    }
    case LogicalTypeID::INT16: {
        return Napi::Number::New(env, value.getValue<int16_t>());
    }
    case LogicalTypeID::INT32: {
        return Napi::Number::New(env, value.getValue<int32_t>());
    }
    case LogicalTypeID::INT64:
    case LogicalTypeID::SERIAL: {
        return Napi::Number::New(env, value.getValue<int64_t>());
    }
    case LogicalTypeID::FLOAT: {
        return Napi::Number::New(env, value.getValue<float>());
    }
    case LogicalTypeID::DOUBLE: {
        return Napi::Number::New(env, value.getValue<double>());
    }
    case LogicalTypeID::STRING: {
        return Napi::String::New(env, value.getValue<std::string>());
    }
    case LogicalTypeID::BLOB: {
        auto blobVal = value.getValue<std::string>();
        return Napi::Buffer<char>::Copy(env, blobVal.c_str(), blobVal.size());
    }
    case LogicalTypeID::DATE: {
        auto dateVal = value.getValue<date_t>();
        // Javascript Date type contains both date and time information. This returns the Date at
        // 00:00:00 in UTC timezone.
        auto milliseconds = Date::getEpochNanoSeconds(dateVal) / Interval::NANOS_PER_MICRO /
                            Interval::MICROS_PER_MSEC;
        return Napi::Date::New(env, milliseconds);
    }
    case LogicalTypeID::TIMESTAMP: {
        auto timestampVal = value.getValue<timestamp_t>();
        auto milliseconds = timestampVal.value / Interval::MICROS_PER_MSEC;
        return Napi::Date::New(env, milliseconds);
    }
    case LogicalTypeID::INTERVAL: {
        // TODO: By default, Node.js returns the difference in milliseconds between two dates, so we
        // follow the convention here, but it might not be the best choice in terms of usability.
        auto intervalVal = value.getValue<interval_t>();
        auto microseconds = intervalVal.micros;
        microseconds += intervalVal.days * Interval::MICROS_PER_DAY;
        microseconds += intervalVal.months * Interval::MICROS_PER_MONTH;
        auto milliseconds = microseconds / Interval::MICROS_PER_MSEC;
        return Napi::Number::New(env, milliseconds);
    }
    case LogicalTypeID::VAR_LIST:
    case LogicalTypeID::FIXED_LIST: {
        auto& listVal = value.getListValReference();
        auto napiArray = Napi::Array::New(env, listVal.size());
        for (auto i = 0u; i < listVal.size(); ++i) {
            napiArray.Set(i, ConvertToNapiObject(*listVal[i], env));
        }
        return napiArray;
    }
    case LogicalTypeID::STRUCT:
    case LogicalTypeID::UNION: {
        auto childrenNames = StructType::getFieldNames(&dataType);
        auto napiObj = Napi::Object::New(env);
        auto& structVal = value.getListValReference();
        for (auto i = 0u; i < structVal.size(); ++i) {
            auto key = childrenNames[i];
            auto val = ConvertToNapiObject(*structVal[i], env);
            napiObj.Set(key, val);
        }
        return napiObj;
    }
    case LogicalTypeID::RECURSIVE_REL: {
        auto napiObj = Napi::Object::New(env);
        napiObj.Set("_nodes", ConvertToNapiObject(*value.getListValReference()[0], env));
        napiObj.Set("_rels", ConvertToNapiObject(*value.getListValReference()[1], env));
        return napiObj;
    }
    case LogicalTypeID::NODE: {
        Napi::Object napiObj = Napi::Object::New(env);
        auto numProperties = NodeVal::getNumProperties(&value);
        for (auto i = 0u; i < numProperties; ++i) {
            auto key = NodeVal::getPropertyName(&value, i);
            auto val = ConvertToNapiObject(*NodeVal::getPropertyValueReference(&value, i), env);
            napiObj.Set(key, val);
        }
        napiObj.Set("_label", Napi::String::New(env, NodeVal::getLabelName(&value)));
        napiObj.Set("_id", ConvertNodeIdToNapiObject(NodeVal::getNodeID(&value), env));
        return napiObj;
    }
    case LogicalTypeID::REL: {
        Napi::Object napiObj = Napi::Object::New(env);
        auto numProperties = RelVal::getNumProperties(&value);
        for (auto i = 0u; i < numProperties; ++i) {
            auto key = RelVal::getPropertyName(&value, i);
            auto val = ConvertToNapiObject(*RelVal::getPropertyValueReference(&value, i), env);
            napiObj.Set(key, val);
        }
        napiObj.Set("_src", ConvertNodeIdToNapiObject(RelVal::getSrcNodeID(&value), env));
        napiObj.Set("_dst", ConvertNodeIdToNapiObject(RelVal::getDstNodeID(&value), env));
        return napiObj;
    }
    case LogicalTypeID::INTERNAL_ID: {
        return ConvertNodeIdToNapiObject(value.getValue<nodeID_t>(), env);
    }
    default:
        throw Exception("Unsupported type: " + LogicalTypeUtils::dataTypeToString(dataType));
    }
    return Napi::Value();
}

std::unordered_map<std::string, std::shared_ptr<Value>> Util::TransformParametersForExec(
    Napi::Array params, std::unordered_map<std::string, std::shared_ptr<Value>>& parameterMap) {
    std::unordered_map<std::string, std::shared_ptr<Value>> result;
    for (size_t i = 0; i < params.Length(); i++) {
        auto param = params.Get(i).As<Napi::Array>();
        assert(param.Length() == 2);
        assert(param.Get(uint32_t(0)).IsString());
        auto key = param.Get(uint32_t(0)).ToString().Utf8Value();
        if (!parameterMap.count(key)) {
            throw Exception("Parameter " + key + " is not defined in the prepared statement");
        }
        auto paramValue = parameterMap[key];
        auto napiValue = param.Get(uint32_t(1));
        auto expectedDataType = paramValue->getDataType();
        auto transformedVal = TransformNapiValue(napiValue, expectedDataType, key);
        result[key] = std::make_shared<Value>(transformedVal);
    }
    return result;
}

Napi::Object Util::ConvertNodeIdToNapiObject(const nodeID_t& nodeId, Napi::Env env) {
    Napi::Object napiObject = Napi::Object::New(env);
    napiObject.Set("offset", Napi::Number::New(env, nodeId.offset));
    napiObject.Set("table", Napi::Number::New(env, nodeId.tableID));
    return napiObject;
}

Value Util::TransformNapiValue(
    Napi::Value napiValue, LogicalType expectedDataType, const std::string& key) {
    switch (expectedDataType.getLogicalTypeID()) {
    case LogicalTypeID::BOOL: {
        return Value(napiValue.ToBoolean().Value());
    }
    case LogicalTypeID::INT16: {
        if (!napiValue.IsNumber()) {
            throw Exception("Expected a number for parameter " + key + ".");
        }
        int16_t val = napiValue.ToNumber().Int32Value();
        return Value(val);
    }
    case LogicalTypeID::INT32: {
        if (!napiValue.IsNumber()) {
            throw Exception("Expected a number for parameter " + key + ".");
        }
        int32_t val = napiValue.ToNumber().Int32Value();
        return Value(val);
    }
    case LogicalTypeID::INT64: {
        if (!napiValue.IsNumber()) {
            throw Exception("Expected a number for parameter " + key + ".");
        }
        int64_t val = napiValue.ToNumber().Int64Value();
        return Value(val);
    }
    case LogicalTypeID::FLOAT: {
        if (!napiValue.IsNumber()) {
            throw Exception("Expected a number for parameter " + key + ".");
        }
        float val = napiValue.ToNumber().FloatValue();
        return Value(val);
    }
    case LogicalTypeID::DOUBLE: {
        if (!napiValue.IsNumber()) {
            throw Exception("Expected a number for parameter " + key + ".");
        }
        double val = napiValue.ToNumber().DoubleValue();
        return Value(val);
    }
    case LogicalTypeID::STRING: {
        std::string val = napiValue.ToString().Utf8Value();
        return Value(LogicalType{LogicalTypeID::STRING}, val);
    }
    case LogicalTypeID::DATE: {
        if (!napiValue.IsDate()) {
            throw Exception("Expected a date for parameter " + key + ".");
        }
        auto napiDate = napiValue.As<Napi::Date>();
        timestamp_t timestamp = Timestamp::FromEpochMs(napiDate.ValueOf());
        auto dateVal = Timestamp::GetDate(timestamp);
        return Value(dateVal);
    }
    case LogicalTypeID::TIMESTAMP: {
        if (!napiValue.IsDate()) {
            throw Exception("Expected a date for parameter " + key + ".");
        }
        auto napiDate = napiValue.As<Napi::Date>();
        timestamp_t timestamp = Timestamp::FromEpochMs(napiDate.ValueOf());
        return Value(timestamp);
    }
    case LogicalTypeID::INTERVAL: {
        if (!napiValue.IsNumber()) {
            throw Exception("Expected a number for parameter " + key + ".");
        }
        auto napiInterval = napiValue.ToNumber().Int64Value();
        auto microseconds = napiInterval * Interval::MICROS_PER_MSEC;
        auto intervalVal = interval_t(0, 0, microseconds);
        int64_t normalizedMonths, normalizedDays, normalizedMicros;
        Interval::NormalizeIntervalEntries(
            intervalVal, normalizedMonths, normalizedDays, normalizedMicros);
        auto normalizedInterval = interval_t(normalizedMonths, normalizedDays, normalizedMicros);
        return Value(normalizedInterval);
    }
    default:
        throw Exception("Unsupported type " + LogicalTypeUtils::dataTypeToString(expectedDataType) +
                        " for parameter: " + key);
    }
}
