#include "include/node_util.h"

#include "common/exception/exception.h"
#include "common/types/blob.h"
#include "common/types/value/nested.h"
#include "common/types/value/node.h"
#include "common/types/value/rdf_variant.h"
#include "common/types/value/recursive_rel.h"
#include "common/types/value/rel.h"
#include "common/types/value/value.h"

Napi::Value ConvertRdfVariantToNapiObject(const Value& value, Napi::Env env) {
    auto type = RdfVariant::getLogicalTypeID(&value);
    switch (type) {
    case LogicalTypeID::STRING: {
        auto str = RdfVariant::getValue<std::string>(&value);
        return Napi::String::New(env, str.data(), str.size());
    }
    case LogicalTypeID::BLOB: {
        auto blobStr = RdfVariant::getValue<blob_t>(&value).value.getAsString();
        return Napi::Buffer<char>::Copy(env, blobStr.c_str(), blobStr.size());
    }
    case LogicalTypeID::INT64: {
        return Napi::Number::New(env, RdfVariant::getValue<int64_t>(&value));
    }
    case LogicalTypeID::INT32: {
        return Napi::Number::New(env, RdfVariant::getValue<int32_t>(&value));
    }
    case LogicalTypeID::INT16: {
        return Napi::Number::New(env, RdfVariant::getValue<int16_t>(&value));
    }
    case LogicalTypeID::INT8: {
        return Napi::Number::New(env, RdfVariant::getValue<int8_t>(&value));
    }
    case LogicalTypeID::UINT64: {
        return Napi::Number::New(env, RdfVariant::getValue<uint64_t>(&value));
    }
    case LogicalTypeID::UINT32: {
        return Napi::Number::New(env, RdfVariant::getValue<uint32_t>(&value));
    }
    case LogicalTypeID::UINT16: {
        return Napi::Number::New(env, RdfVariant::getValue<uint16_t>(&value));
    }
    case LogicalTypeID::UINT8: {
        return Napi::Number::New(env, RdfVariant::getValue<uint8_t>(&value));
    }
    case LogicalTypeID::DOUBLE: {
        return Napi::Number::New(env, RdfVariant::getValue<double>(&value));
    }
    case LogicalTypeID::FLOAT: {
        return Napi::Number::New(env, RdfVariant::getValue<float>(&value));
    }
    case LogicalTypeID::BOOL: {
        return Napi::Boolean::New(env, RdfVariant::getValue<bool>(&value));
    }
    case LogicalTypeID::DATE: {
        auto dateVal = RdfVariant::getValue<date_t>(&value);
        // Javascript Date type contains both date and time information. This returns the Date at
        // 00:00:00 in UTC timezone.
        auto milliseconds = Date::getEpochNanoSeconds(dateVal) / Interval::NANOS_PER_MICRO /
                            Interval::MICROS_PER_MSEC;
        return Napi::Date::New(env, milliseconds);
    }
    case LogicalTypeID::TIMESTAMP: {
        auto timestampVal = RdfVariant::getValue<timestamp_t>(&value);
        auto milliseconds = timestampVal.value / Interval::MICROS_PER_MSEC;
        return Napi::Date::New(env, milliseconds);
    }
    case LogicalTypeID::INTERVAL: {
        auto intervalVal = RdfVariant::getValue<interval_t>(&value);
        // TODO: By default, Node.js returns the difference in milliseconds between two dates, so we
        // follow the convention here, but it might not be the best choice in terms of usability.
        auto microseconds = intervalVal.micros;
        microseconds += intervalVal.days * Interval::MICROS_PER_DAY;
        microseconds += intervalVal.months * Interval::MICROS_PER_MONTH;
        auto milliseconds = microseconds / Interval::MICROS_PER_MSEC;
        return Napi::Number::New(env, milliseconds);
    }
    default: {
        KU_UNREACHABLE;
    }
    }
}

Napi::Value Util::ConvertToNapiObject(const Value& value, Napi::Env env) {
    if (value.isNull()) {
        return env.Null();
    }
    const auto& dataType = value.getDataType();
    auto dataTypeID = dataType.getLogicalTypeID();
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
    case LogicalTypeID::UUID:
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
    case LogicalTypeID::TIMESTAMP_TZ: {
        auto timestampVal = value.getValue<timestamp_tz_t>();
        auto milliseconds = timestampVal.value / Interval::MICROS_PER_MSEC;
        return Napi::Date::New(env, milliseconds);
    }
    case LogicalTypeID::TIMESTAMP: {
        auto timestampVal = value.getValue<timestamp_t>();
        auto milliseconds = timestampVal.value / Interval::MICROS_PER_MSEC;
        return Napi::Date::New(env, milliseconds);
    }
    case LogicalTypeID::TIMESTAMP_NS: {
        auto timestampVal = value.getValue<timestamp_ns_t>();
        auto milliseconds =
            timestampVal.value / Interval::NANOS_PER_MICRO / Interval::MICROS_PER_MSEC;
        return Napi::Date::New(env, milliseconds);
    }
    case LogicalTypeID::TIMESTAMP_MS: {
        auto timestampVal = value.getValue<timestamp_ms_t>();
        return Napi::Date::New(env, timestampVal.value);
    }
    case LogicalTypeID::TIMESTAMP_SEC: {
        auto timestampVal = value.getValue<timestamp_sec_t>();
        auto milliseconds =
            Timestamp::getEpochMilliSeconds(Timestamp::fromEpochSeconds(timestampVal.value));
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
    case LogicalTypeID::LIST:
    case LogicalTypeID::ARRAY: {
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
    case LogicalTypeID::RDF_VARIANT: {
        return ConvertRdfVariantToNapiObject(value, env);
    }
    default:
        throw Exception("Unsupported type: " + dataType.toString());
    }
    return Napi::Value();
}

std::unordered_map<std::string, std::unique_ptr<Value>> Util::TransformParametersForExec(
    Napi::Array params) {
    std::unordered_map<std::string, std::unique_ptr<Value>> result;
    for (size_t i = 0; i < params.Length(); i++) {
        auto param = params.Get(i).As<Napi::Array>();
        KU_ASSERT(param.Length() == 2);
        KU_ASSERT(param.Get(uint32_t(0)).IsString());
        auto key = param.Get(uint32_t(0)).ToString().Utf8Value();
        auto napiValue = param.Get(uint32_t(1));
        auto transformedVal = TransformNapiValue(napiValue);
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

Value Util::TransformNapiValue(Napi::Value napiValue) {
    if (napiValue.IsBoolean()) {
        return Value(napiValue.ToBoolean().Value());
    }
    if (napiValue.IsBigInt()) {
        auto bigInt = napiValue.As<Napi::BigInt>();
        size_t wordsCount = bigInt.WordCount();
        std::unique_ptr<uint64_t[]> words(new uint64_t[wordsCount]());
        int signBit = 0;
        bigInt.ToWords(&signBit, &wordsCount, words.get());
        kuzu::common::int128_t val;
        val.low = words[0];
        val.high = wordsCount > 1 ? words[1] : 0;
        // Ignore words[2] and beyond as we only support 128-bit integers but BigInt can be larger.
        if (signBit) {
            kuzu::common::Int128_t::negateInPlace(val);
        }
        return Value(val);
    }
    if (napiValue.IsNumber()) {
        auto num = napiValue.ToNumber();
        auto doubleVal = num.DoubleValue();
        if (doubleVal > JS_MAX_SAFE_INTEGER || doubleVal < JS_MIN_SAFE_INTEGER) {
            return Value(doubleVal);
        }
        auto intVal = num.Int64Value();
        return intVal == doubleVal ? Value(intVal) : Value(doubleVal);
    }
    if (napiValue.IsString()) {
        return Value(napiValue.ToString().Utf8Value());
    }
    if (napiValue.IsDate()) {
        auto napiDate = napiValue.As<Napi::Date>();
        timestamp_t timestamp = Timestamp::fromEpochMilliSeconds(napiDate.ValueOf());
        auto dateVal = Timestamp::getDate(timestamp);
        return Value(dateVal);
    }
    return Value::createNullValue();
}
