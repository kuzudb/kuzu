#include "include/node_util.h"

#include "common/exception/exception.h"
#include "common/types/value/nested.h"
#include "common/types/value/node.h"
#include "common/types/value/recursive_rel.h"
#include "common/types/value/rel.h"
#include "common/types/value/value.h"

Napi::Value Util::ConvertToNapiObject(const Value& value, Napi::Env env) {
    if (value.isNull()) {
        return env.Null();
    }
    auto dataType = value.getDataType();
    auto dataTypeID = dataType->getLogicalTypeID();
    switch (dataTypeID) {
    case LogicalTypeID::BOOL: {
        return Napi::Boolean::New(env, value.getValue<bool>());
    }
    case LogicalTypeID::UINT8: {
        return Napi::Number::New(env, value.getValue<uint8_t>());
    }
    case LogicalTypeID::UINT16: {
        return Napi::Number::New(env, value.getValue<uint16_t>());
    }
    case LogicalTypeID::UINT32: {
        return Napi::Number::New(env, value.getValue<uint32_t>());
    }
    case LogicalTypeID::UINT64: {
        return Napi::Number::New(env, value.getValue<uint64_t>());
    }
    case LogicalTypeID::INT8: {
        return Napi::Number::New(env, value.getValue<int8_t>());
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
    case LogicalTypeID::INT128: {
        auto val = value.getValue<kuzu::common::int128_t>();
        auto negative = val.high < 0;
        if (negative) {
            kuzu::common::Int128_t::negateInPlace(val);
        }
        const uint64_t words[] = {val.low, static_cast<uint64_t>(val.high)};
        return Napi::BigInt::New(env, negative, 2, words);
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
        auto size = NestedVal::getChildrenSize(&value);
        auto napiArray = Napi::Array::New(env, size);
        for (auto i = 0u; i < size; ++i) {
            auto childVal = NestedVal::getChildVal(&value, i);
            napiArray.Set(i, ConvertToNapiObject(*childVal, env));
        }
        return napiArray;
    }
    case LogicalTypeID::STRUCT:
    case LogicalTypeID::UNION: {
        auto childrenNames = StructType::getFieldNames(dataType);
        auto napiObj = Napi::Object::New(env);
        auto size = NestedVal::getChildrenSize(&value);
        for (auto i = 0u; i < size; ++i) {
            auto key = childrenNames[i];
            auto childVal = NestedVal::getChildVal(&value, i);
            auto val = ConvertToNapiObject(*childVal, env);
            napiObj.Set(key, val);
        }
        return napiObj;
    }
    case LogicalTypeID::RECURSIVE_REL: {
        auto napiObj = Napi::Object::New(env);
        auto nodes = RecursiveRelVal::getNodes(&value);
        auto rels = RecursiveRelVal::getRels(&value);
        napiObj.Set("_nodes", ConvertToNapiObject(*nodes, env));
        napiObj.Set("_rels", ConvertToNapiObject(*rels, env));
        return napiObj;
    }
    case LogicalTypeID::NODE: {
        Napi::Object napiObj = Napi::Object::New(env);
        auto numProperties = NodeVal::getNumProperties(&value);
        for (auto i = 0u; i < numProperties; ++i) {
            auto key = NodeVal::getPropertyName(&value, i);
            auto val = ConvertToNapiObject(*NodeVal::getPropertyVal(&value, i), env);
            napiObj.Set(key, val);
        }
        auto labelVal = NodeVal::getLabelVal(&value);
        auto idVal = NodeVal::getNodeIDVal(&value);
        auto label =
            labelVal ? Napi::String::New(env, labelVal->getValue<std::string>()) : env.Null();
        auto id = idVal ? ConvertToNapiObject(*idVal, env) : env.Null();
        napiObj.Set("_label", label);
        napiObj.Set("_id", id);
        return napiObj;
    }
    case LogicalTypeID::REL: {
        Napi::Object napiObj = Napi::Object::New(env);
        auto numProperties = RelVal::getNumProperties(&value);
        for (auto i = 0u; i < numProperties; ++i) {
            auto key = RelVal::getPropertyName(&value, i);
            auto val = ConvertToNapiObject(*RelVal::getPropertyVal(&value, i), env);
            napiObj.Set(key, val);
        }
        auto srcIdVal = RelVal::getSrcNodeIDVal(&value);
        auto dstIdVal = RelVal::getDstNodeIDVal(&value);
        auto labelVal = RelVal::getLabelVal(&value);
        auto srcId =
            srcIdVal ? ConvertNodeIdToNapiObject(srcIdVal->getValue<nodeID_t>(), env) : env.Null();
        auto dstId =
            dstIdVal ? ConvertNodeIdToNapiObject(dstIdVal->getValue<nodeID_t>(), env) : env.Null();
        auto label =
            labelVal ? Napi::String::New(env, labelVal->getValue<std::string>()) : env.Null();
        napiObj.Set("_src", srcId);
        napiObj.Set("_dst", dstId);
        napiObj.Set("_label", label);
        return napiObj;
    }
    case LogicalTypeID::INTERNAL_ID: {
        return ConvertNodeIdToNapiObject(value.getValue<nodeID_t>(), env);
    }
    case LogicalTypeID::MAP: {
        Napi::Object napiObj = Napi::Object::New(env);
        for (auto i = 0u; i < NestedVal::getChildrenSize(&value); ++i) {
            auto childVal = NestedVal::getChildVal(&value, i);
            auto key = NestedVal::getChildVal(childVal, 0)->toString();
            auto val = ConvertToNapiObject(*NestedVal::getChildVal(childVal, 1), env);
            napiObj.Set(key, val);
        }
        return napiObj;
    }
    default:
        throw Exception("Unsupported type: " + dataType->toString());
    }
    return Napi::Value();
}

std::unordered_map<std::string, std::unique_ptr<Value>> Util::TransformParametersForExec(
    Napi::Array params,
    const std::unordered_map<std::string, std::shared_ptr<Value>>& parameterMap) {
    std::unordered_map<std::string, std::unique_ptr<Value>> result;
    for (size_t i = 0; i < params.Length(); i++) {
        auto param = params.Get(i).As<Napi::Array>();
        KU_ASSERT(param.Length() == 2);
        KU_ASSERT(param.Get(uint32_t(0)).IsString());
        auto key = param.Get(uint32_t(0)).ToString().Utf8Value();
        if (!parameterMap.count(key)) {
            throw Exception("Parameter " + key + " is not defined in the prepared statement");
        }
        auto napiValue = param.Get(uint32_t(1));
        auto expectedDataType = parameterMap.at(key)->getDataType();
        auto transformedVal = TransformNapiValue(napiValue, expectedDataType, key);
        result[key] = std::make_unique<Value>(transformedVal);
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
    Napi::Value napiValue, LogicalType* expectedDataType, const std::string& key) {
    auto logicalTypeId = expectedDataType->getLogicalTypeID();
    switch (logicalTypeId) {
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
        timestamp_t timestamp = Timestamp::fromEpochMilliSeconds(napiDate.ValueOf());
        auto dateVal = Timestamp::getDate(timestamp);
        return Value(dateVal);
    }
    case LogicalTypeID::TIMESTAMP: {
        if (!napiValue.IsDate()) {
            throw Exception("Expected a date for parameter " + key + ".");
        }
        auto napiDate = napiValue.As<Napi::Date>();
        timestamp_t timestamp = Timestamp::fromEpochMilliSeconds(napiDate.ValueOf());
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
        Interval::normalizeIntervalEntries(
            intervalVal, normalizedMonths, normalizedDays, normalizedMicros);
        auto normalizedInterval = interval_t(normalizedMonths, normalizedDays, normalizedMicros);
        return Value(normalizedInterval);
    }
    default:
        throw Exception(
            "Unsupported type " + expectedDataType->toString() + " for parameter: " + key);
    }
}
