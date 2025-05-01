#include <cmath>

#include "common/constants.h"
#include "common/exception/exception.h"
#include "common/exception/not_implemented.h"
#include "function/cast/functions/cast_from_string_functions.h"
#include "function/cast/functions/cast_string_non_nested_functions.h"
#include "main/kuzu.h"
#include <emscripten/bind.h>
#include <emscripten/val.h>
using namespace emscripten;
using namespace kuzu::main;
using namespace kuzu::common;
using namespace kuzu::processor;

// Dummy main function to make Emscripten happy
int main() {
    return 0;
}

val JsDate = val::global("Date");
val JsNumber = val::global("Number");
val JsBigInt = val::global("BigInt");
val JsArray = val::global("Array");
val JsObject = val::global("Object");

/**
 * Helper functions
 */
QueryResult* connectionQueryWrapper(Connection& conn, std::string query) {
    return conn.query(std::string_view(query)).release();
}

PreparedStatement* connectionPrepareWrapper(Connection& conn, std::string query) {
    return conn.prepare(std::string_view(query)).release();
}

Value valueFromEmscriptenValue(const val& value) {
    if (value.isNull() || value.isUndefined()) {
        return Value::createNullValue();
    }
    auto type = value.typeOf().as<std::string>();
    if (type == "boolean") {
        return Value(value.as<bool>());
    }
    if (type == "bigint") {
        auto str = value.call<val>("toString").as<std::string>();
        int128_t int128_val = 0;
        kuzu::function::CastString::operation(ku_string_t{str.c_str(), str.size()}, int128_val);
        return Value(int128_val);
    }
    if (type == "number") {
        auto maxSafeInteger = JsNumber["MAX_SAFE_INTEGER"].as<int64_t>();
        auto minSafeInteger = JsNumber["MIN_SAFE_INTEGER"].as<int64_t>();
        auto doubleVal = value.as<double>();
        if (doubleVal > maxSafeInteger || doubleVal < minSafeInteger) {
            return Value(doubleVal);
        } else if (std::floor(doubleVal) == doubleVal) {
            return Value(value.as<int64_t>());
        } else {
            return Value(doubleVal);
        }
    }
    if (type == "string") {
        return Value(value.as<std::string>());
    }
    if (JsArray["isArray"](value).as<bool>()) {
        auto size = value["length"].as<unsigned>();
        if (size == 0) {
            return Value::createNullValue();
        }
        std::vector<std::unique_ptr<Value>> children;
        for (size_t i = 0; i < size; ++i) {
            children.push_back(std::make_unique<Value>(valueFromEmscriptenValue(value[i])));
        }
        auto dataType = LogicalType::LIST(children[0]->getDataType().copy());
        return Value(std::move(dataType), std::move(children));
    }
    if (type == "object") {
        if (value.instanceof (JsDate)) {
            auto milliseconds = value.call<val>("getTime").as<int64_t>();
            timestamp_t timestampVal = Timestamp::fromEpochMilliSeconds(milliseconds);
            return Value(timestampVal);
        } else {
            auto keys = JsObject["keys"](value);
            auto size = keys["length"].as<unsigned>();
            if (size == 0) {
                return Value::createNullValue();
            }
            auto struct_fields = std::vector<StructField>{};
            std::vector<std::unique_ptr<Value>> children;
            for (size_t i = 0; i < size; ++i) {
                auto key = keys[i].as<std::string>();
                auto val = valueFromEmscriptenValue(value[key]);
                struct_fields.emplace_back(key, val.getDataType().copy());
                children.push_back(std::make_unique<Value>(val));
            }
            auto dataType = LogicalType::STRUCT(std::move(struct_fields));
            return Value(std::move(dataType), std::move(children));
        }
    }
    throw Exception("Unsupported type");
}

std::unordered_map<std::string, std::unique_ptr<Value>> transformParametersForExec(
    const val& params) {
    std::unordered_map<std::string, std::unique_ptr<Value>> result;
    if (!JsArray["isArray"](params)) {
        throw Exception("Parameters must be an array");
    }
    auto length = params["length"].as<unsigned>();
    for (size_t i = 0; i < length; ++i) {
        val param = params[i];
        if (param.typeOf().as<std::string>() != "object") {
            throw Exception("Each parameter must be an object");
        }
        if (!param.hasOwnProperty("name") || param["name"].typeOf().as<std::string>() != "string") {
            throw Exception("Each parameter must have a 'name' property of type string");
        }
        if (!param.hasOwnProperty("value")) {
            throw Exception("Each parameter must have a 'value' property");
        }
        auto name = param["name"].as<std::string>();
        auto value = param["value"];
        auto transformedValue = valueFromEmscriptenValue(value);
        result[name] = std::make_unique<Value>(transformedValue);
    }
    return result;
}

QueryResult* connectionExecuteWrapper(Connection& conn, PreparedStatement* preparedStatement,
    val params) {
    auto paramsMap = transformParametersForExec(params);
    return conn.executeWithParams(preparedStatement, std::move(paramsMap)).release();
};

val queryResultGetColumnTypes(QueryResult& queryResult) {
    const auto& columnTypes = queryResult.getColumnDataTypes();
    auto jsArray = val::array();
    for (const auto& columnType : columnTypes) {
        jsArray.call<void>("push", columnType.toString());
    }
    return jsArray;
}

val queryResultGetColumnNames(QueryResult& queryResult) {
    const auto& columnNames = queryResult.getColumnNames();
    auto jsArray = val::array();
    for (const auto& columnName : columnNames) {
        jsArray.call<void>("push", columnName);
    }
    return jsArray;
}

val queryResultGetQuerySummary(QueryResult& queryResult) {
    const auto& querySummary = queryResult.getQuerySummary();
    auto jsObject = val::object();
    jsObject.set("executionTime", querySummary->getExecutionTime());
    jsObject.set("compilingTime", querySummary->getCompilingTime());
    return jsObject;
}

val valueGetAsEmscriptenValue(const Value& value) {
    if (value.isNull()) {
        return val::null();
    }
    const auto& dataType = value.getDataType();
    auto dataTypeID = dataType.getLogicalTypeID();
    switch (dataTypeID) {
    case LogicalTypeID::BOOL: {
        return val(value.getValue<bool>());
    }
    case LogicalTypeID::UINT8: {
        return JsNumber.new_(value.getValue<uint8_t>());
    }
    case LogicalTypeID::UINT16: {
        return JsNumber.new_(value.getValue<uint16_t>());
    }
    case LogicalTypeID::UINT32: {
        return JsNumber.new_(value.getValue<uint32_t>());
    }
    case LogicalTypeID::UINT64: {
        auto uint64tVal = value.getValue<uint64_t>();
        if (uint64tVal > JsNumber["MAX_SAFE_INTEGER"].as<uint64_t>()) {
            return val(uint64tVal);
        }
        return JsNumber.new_(uint64tVal);
    }
    case LogicalTypeID::INT8: {
        return JsNumber.new_(value.getValue<int8_t>());
    }
    case LogicalTypeID::INT16: {
        return JsNumber.new_(value.getValue<int16_t>());
    }
    case LogicalTypeID::INT32: {
        return JsNumber.new_(value.getValue<int32_t>());
    }
    case LogicalTypeID::INT64:
    case LogicalTypeID::SERIAL: {
        auto int64Val = value.getValue<int64_t>();
        if (int64Val > JsNumber["MAX_SAFE_INTEGER"].as<int64_t>() ||
            int64Val < JsNumber["MIN_SAFE_INTEGER"].as<int64_t>()) {
            return val(int64Val);
        }
        return JsNumber.new_(int64Val);
    }
    case LogicalTypeID::INT128: {
        auto int128Val = value.getValue<int128_t>();
        auto str = Int128_t::ToString(int128Val);
        return JsBigInt(str);
    }
    case LogicalTypeID::FLOAT: {
        return val(value.getValue<float>());
    }
    case LogicalTypeID::DOUBLE: {
        return val(value.getValue<double>());
    }
    case LogicalTypeID::UUID:
    case LogicalTypeID::STRING: {
        return val(value.getValue<std::string>());
    }
    case LogicalTypeID::BLOB: {
        auto blobVal = value.getValue<std::string>(); // Get the BLOB data
        size_t size = blobVal.size();
        val uint8Array = val::global("Uint8Array").new_(size);
        val memoryView = val(typed_memory_view(size, blobVal.data()));
        uint8Array.call<void>("set", memoryView);
        return uint8Array;
    }
    case LogicalTypeID::DATE: {
        auto dateVal = value.getValue<date_t>();
        auto days = dateVal.days;
        auto millisecondsPerDay = Interval::MICROS_PER_DAY / Interval::MICROS_PER_MSEC;
        int64_t milliseconds = days * millisecondsPerDay;
        return JsDate.new_(JsNumber.new_(milliseconds));
    }
    case LogicalTypeID::TIMESTAMP_TZ: {
        auto timestampVal = value.getValue<timestamp_tz_t>();
        auto milliseconds = timestampVal.value / Interval::MICROS_PER_MSEC;
        return JsDate.new_(JsNumber.new_(milliseconds));
    }
    case LogicalTypeID::TIMESTAMP: {
        auto timestampVal = value.getValue<timestamp_t>();
        auto milliseconds = timestampVal.value / Interval::MICROS_PER_MSEC;
        return JsDate.new_(JsNumber.new_(milliseconds));
    }
    case LogicalTypeID::TIMESTAMP_NS: {
        auto timestampVal = value.getValue<timestamp_ns_t>();
        auto milliseconds =
            timestampVal.value / Interval::NANOS_PER_MICRO / Interval::MICROS_PER_MSEC;
        return JsDate.new_(JsNumber.new_(milliseconds));
    }
    case LogicalTypeID::TIMESTAMP_MS: {
        auto timestampVal = value.getValue<timestamp_ms_t>();
        return JsDate.new_(JsNumber.new_(timestampVal.value));
    }
    case LogicalTypeID::TIMESTAMP_SEC: {
        auto timestampVal = value.getValue<timestamp_sec_t>();
        auto milliseconds =
            Timestamp::getEpochMilliSeconds(Timestamp::fromEpochSeconds(timestampVal.value));
        return JsDate.new_(JsNumber.new_(milliseconds));
    }
    case LogicalTypeID::INTERVAL: {
        auto intervalVal = value.getValue<interval_t>();
        auto microseconds = intervalVal.micros;
        microseconds += intervalVal.days * Interval::MICROS_PER_DAY;
        microseconds += intervalVal.months * Interval::MICROS_PER_MONTH;
        auto milliseconds = microseconds / Interval::MICROS_PER_MSEC;
        return val(milliseconds);
    }
    case LogicalTypeID::LIST:
    case LogicalTypeID::ARRAY: {
        auto size = NestedVal::getChildrenSize(&value);
        auto jsArray = val::array();
        for (auto i = 0u; i < size; ++i) {
            auto childVal = NestedVal::getChildVal(&value, i);
            jsArray.set(i, valueGetAsEmscriptenValue(*childVal));
        }
        return jsArray;
    }
    case LogicalTypeID::STRUCT:
    case LogicalTypeID::UNION: {
        auto jsObject = val::object();
        auto childrenNames = StructType::getFieldNames(dataType);
        auto size = NestedVal::getChildrenSize(&value);
        for (auto i = 0u; i < size; ++i) {
            auto key = childrenNames[i];
            auto childVal = NestedVal::getChildVal(&value, i);
            auto val = valueGetAsEmscriptenValue(*childVal);
            jsObject.set(key, val);
        }
        return jsObject;
    }
    case LogicalTypeID::RECURSIVE_REL: {
        auto jsObject = val::object();
        auto nodes = RecursiveRelVal::getNodes(&value);
        auto rels = RecursiveRelVal::getRels(&value);
        jsObject.set("_nodes", valueGetAsEmscriptenValue(*nodes));
        jsObject.set("_rels", valueGetAsEmscriptenValue(*rels));
        return jsObject;
    }
    case LogicalTypeID::NODE: {
        auto jsObject = val::object();
        auto numProperties = NodeVal::getNumProperties(&value);
        for (auto i = 0u; i < numProperties; ++i) {
            auto key = NodeVal::getPropertyName(&value, i);
            auto val = valueGetAsEmscriptenValue(*NodeVal::getPropertyVal(&value, i));
            jsObject.set(key, val);
        }
        auto labelVal = NodeVal::getLabelVal(&value);
        auto idVal = NodeVal::getNodeIDVal(&value);
        auto label = labelVal ? val(labelVal->getValue<std::string>()) : val::null();
        auto id = idVal ? valueGetAsEmscriptenValue(*idVal) : val::null();
        jsObject.set("_label", label);
        jsObject.set("_id", id);
        return jsObject;
    }
    case LogicalTypeID::REL: {
        auto jsObject = val::object();
        auto numProperties = RelVal::getNumProperties(&value);
        for (auto i = 0u; i < numProperties; ++i) {
            auto key = RelVal::getPropertyName(&value, i);
            auto val = valueGetAsEmscriptenValue(*RelVal::getPropertyVal(&value, i));
            jsObject.set(key, val);
        }
        auto srcIdVal = RelVal::getSrcNodeIDVal(&value);
        auto dstIdVal = RelVal::getDstNodeIDVal(&value);
        auto labelVal = RelVal::getLabelVal(&value);
        auto srcId = srcIdVal ? valueGetAsEmscriptenValue(*srcIdVal) : val::null();
        auto dstId = dstIdVal ? valueGetAsEmscriptenValue(*dstIdVal) : val::null();
        auto label = labelVal ? val(labelVal->getValue<std::string>()) : val::null();
        auto idVal = RelVal::getIDVal(&value);
        auto id = idVal ? valueGetAsEmscriptenValue(*idVal) : val::null();
        jsObject.set("_src", srcId);
        jsObject.set("_dst", dstId);
        jsObject.set("_label", label);
        jsObject.set("_id", id);
        return jsObject;
    }
    case LogicalTypeID::INTERNAL_ID: {
        auto jsObject = val::object();
        auto id = value.getValue<nodeID_t>();
        jsObject.set("offset", id.offset);
        jsObject.set("table", id.tableID);
        return jsObject;
    }
    case LogicalTypeID::MAP: {
        auto jsObject = val::object();
        for (auto i = 0u; i < NestedVal::getChildrenSize(&value); ++i) {
            auto childVal = NestedVal::getChildVal(&value, i);
            auto key = NestedVal::getChildVal(childVal, 0)->toString();
            auto val = valueGetAsEmscriptenValue(*NestedVal::getChildVal(childVal, 1));
            jsObject.set(key, val);
        }
        return jsObject;
    }
    case LogicalTypeID::DECIMAL: {
        auto valString = value.toString();
        return JsNumber.new_(valString);
    }
    default:
        throw Exception("Unsupported type: " + dataType.toString());
    }
}

val flatTupleGetAsEmscriptenValue(const FlatTuple& flatTuple) {
    auto jsArray = val::array();
    for (auto i = 0u; i < flatTuple.len(); ++i) {
        auto val = flatTuple.getValue(i);
        jsArray.set(i, valueGetAsEmscriptenValue(*val));
    }
    return jsArray;
}

val queryResultGetNext(QueryResult& queryResult) {
    if (!queryResult.hasNext()) {
        return val::null();
    }
    auto flatTuple = queryResult.getNext();
    return flatTupleGetAsEmscriptenValue(*flatTuple);
}

val queryResultGetAsEmscriptenNestedArray(QueryResult& queryResult) {
    auto jsArray = val::array();
    queryResult.resetIterator();
    while (queryResult.hasNext()) {
        auto flatTuple = queryResult.getNext();
        jsArray.call<void>("push", flatTupleGetAsEmscriptenValue(*flatTuple));
    }
    return jsArray;
}

val queryResultGetAsEmscriptenArrayOfObjects(QueryResult& queryResult) {
    auto jsArray = val::array();
    queryResult.resetIterator();
    auto columnNames = queryResult.getColumnNames();
    while (queryResult.hasNext()) {
        auto flatTuple = queryResult.getNext();
        auto jsObject = val::object();
        for (auto i = 0u; i < flatTuple->len(); ++i) {
            auto val = flatTuple->getValue(i);
            jsObject.set(columnNames[i], valueGetAsEmscriptenValue(*val));
        }
        jsArray.call<void>("push", jsObject);
    }
    return jsArray;
}

std::string getVersion() {
    return std::string(Version::getVersion());
}
/**
 * Embind declarations which expose the C++ classes to JavaScript
 */
EMSCRIPTEN_BINDINGS(kuzu_wasm) {
    class_<SystemConfig>("SystemConfig")
        .constructor<uint64_t, uint64_t, bool, bool, uint64_t, bool, uint64_t>()
        .constructor<>()
        .property("bufferPoolSize", &SystemConfig::bufferPoolSize)
        .property("maxNumThreads", &SystemConfig::maxNumThreads)
        .property("enableCompression", &SystemConfig::enableCompression)
        .property("readOnly", &SystemConfig::readOnly)
        .property("maxDBSize", &SystemConfig::maxDBSize)
        .property("autoCheckpoint", &SystemConfig::autoCheckpoint)
        .property("checkpointThreshold", &SystemConfig::checkpointThreshold);
    class_<Database>("Database").constructor<std::string, SystemConfig>();
    class_<PreparedStatement>("PreparedStatement")
        .function("isSuccess", &PreparedStatement::isSuccess)
        .function("getErrorMessage", &PreparedStatement::getErrorMessage);
    class_<Connection>("Connection")
        .constructor<Database*>()
        .function("setMaxNumThreadForExec", &Connection::setMaxNumThreadForExec)
        .function("getMaxNumThreadForExec", &Connection::getMaxNumThreadForExec)
        .function("setQueryTimeOut", &Connection::setQueryTimeOut)
        .function("query", &connectionQueryWrapper, allow_raw_pointers())
        .function("prepare", &connectionPrepareWrapper, allow_raw_pointers())
        .function("execute", &connectionExecuteWrapper, allow_raw_pointers());
    class_<QueryResult>("QueryResult")
        .function("isSuccess", &QueryResult::isSuccess)
        .function("getErrorMessage", &QueryResult::getErrorMessage)
        .function("getNumColumns", &QueryResult::getNumColumns)
        .function("getColumnNames", &queryResultGetColumnNames)
        .function("getColumnTypes", &queryResultGetColumnTypes)
        .function("getNumTuples", &QueryResult::getNumTuples)
        .function("getQuerySummary", &queryResultGetQuerySummary)
        .function("hasNext", &QueryResult::hasNext)
        .function("hasNextQueryResult", &QueryResult::hasNextQueryResult)
        .function("getNext", &queryResultGetNext)
        .function("getNextQueryResult", &QueryResult::getNextQueryResult, allow_raw_pointers())
        .function("toString", &QueryResult::toString)
        .function("resetIterator", &QueryResult::resetIterator)
        .function("getAsJsNestedArray", &queryResultGetAsEmscriptenNestedArray)
        .function("getAsJsArrayOfObjects", &queryResultGetAsEmscriptenArrayOfObjects);
    function("getVersion", &getVersion);
    function("getStorageVersion", &Version::getStorageVersion);
}
