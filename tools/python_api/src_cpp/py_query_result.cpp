#include "include/py_query_result.h"

#include <string>

#include "common/arrow/arrow_converter.h"
#include "common/exception/not_implemented.h"
#include "common/types/uuid.h"
#include "common/types/value/nested.h"
#include "common/types/value/node.h"
#include "common/types/value/rel.h"
#include "datetime.h" // python lib
#include "include/py_query_result_converter.h"

using namespace kuzu::common;

#define PyDateTimeTZ_FromDateAndTime(year, month, day, hour, min, sec, usec, timezone)             \
    PyDateTimeAPI->DateTime_FromDateAndTime(                                                       \
        year, month, day, hour, min, sec, usec, timezone, PyDateTimeAPI->DateTimeType)

void PyQueryResult::initialize(py::handle& m) {
    py::class_<PyQueryResult>(m, "result")
        .def("hasNext", &PyQueryResult::hasNext)
        .def("getNext", &PyQueryResult::getNext)
        .def("writeToCSV", &PyQueryResult::writeToCSV, py::arg("filename"),
            py::arg("delimiter") = ",", py::arg("escapeCharacter") = "\"",
            py::arg("newline") = "\n")
        .def("close", &PyQueryResult::close)
        .def("getAsDF", &PyQueryResult::getAsDF)
        .def("getAsArrow", &PyQueryResult::getAsArrow)
        .def("getColumnNames", &PyQueryResult::getColumnNames)
        .def("getColumnDataTypes", &PyQueryResult::getColumnDataTypes)
        .def("resetIterator", &PyQueryResult::resetIterator)
        .def("isSuccess", &PyQueryResult::isSuccess)
        .def("getErrorMessage", &PyQueryResult::getErrorMessage)
        .def("getCompilingTime", &PyQueryResult::getCompilingTime)
        .def("getExecutionTime", &PyQueryResult::getExecutionTime)
        .def("getNumTuples", &PyQueryResult::getNumTuples);
    // PyDateTime_IMPORT is a macro that must be invoked before calling any other cpython datetime
    // macros. One could also invoke this in a separate function like constructor. See
    // https://docs.python.org/3/c-api/datetime.html for details.
    PyDateTime_IMPORT;
}

bool PyQueryResult::hasNext() {
    return queryResult->hasNext();
}

py::list PyQueryResult::getNext() {
    auto tuple = queryResult->getNext();
    py::tuple result(tuple->len());
    for (auto i = 0u; i < tuple->len(); ++i) {
        result[i] = convertValueToPyObject(*tuple->getValue(i));
    }
    return result;
}

void PyQueryResult::writeToCSV(const py::str& filename, const py::str& delimiter,
    const py::str& escapeCharacter, const py::str& newline) {
    std::string delimiterStr = delimiter;
    std::string escapeCharacterStr = escapeCharacter;
    std::string newlineStr = newline;
    KU_ASSERT(delimiterStr.size() == 1);
    KU_ASSERT(escapeCharacterStr.size() == 1);
    KU_ASSERT(newlineStr.size() == 1);
    queryResult->writeToCSV(
        std::string(filename), delimiterStr[0], escapeCharacterStr[0], newlineStr[0]);
}

void PyQueryResult::close() {
    // Note: Python does not guarantee objects to be deleted in the reverse order. Therefore, we
    // expose close() interface so that users can explicitly call close() and ensure that
    // QueryResult is destroyed before Database.
    queryResult.reset();
}

static py::object converTimestampToPyObject(timestamp_t& timestamp) {
    int32_t year, month, day, hour, min, sec, micros;
    date_t date;
    dtime_t time;
    Timestamp::convert(timestamp, date, time);
    Date::convert(date, year, month, day);
    Time::Convert(time, hour, min, sec, micros);
    return py::cast<py::object>(
        PyDateTime_FromDateAndTime(year, month, day, hour, min, sec, micros));
}

py::object convertRdfVariantToPyObject(const Value& value) {
    auto type = RdfVariant::getLogicalTypeID(&value);
    switch (type) {
    case LogicalTypeID::STRING: {
        return py::str(RdfVariant::getValue<std::string>(&value));
    }
    case LogicalTypeID::BLOB: {
        auto blobStr = RdfVariant::getValue<blob_t>(&value).value.getAsString();
        return py::bytes(blobStr.c_str(), blobStr.size());
    }
    case LogicalTypeID::INT64: {
        return py::cast(RdfVariant::getValue<int64_t>(&value));
    }
    case LogicalTypeID::INT32: {
        return py::cast(RdfVariant::getValue<int32_t>(&value));
    }
    case LogicalTypeID::INT16: {
        return py::cast(RdfVariant::getValue<int16_t>(&value));
    }
    case LogicalTypeID::INT8: {
        return py::cast(RdfVariant::getValue<int8_t>(&value));
    }
    case LogicalTypeID::UINT64: {
        return py::cast(RdfVariant::getValue<uint64_t>(&value));
    }
    case LogicalTypeID::UINT32: {
        return py::cast(RdfVariant::getValue<uint32_t>(&value));
    }
    case LogicalTypeID::UINT16: {
        return py::cast(RdfVariant::getValue<uint16_t>(&value));
    }
    case LogicalTypeID::UINT8: {
        return py::cast(RdfVariant::getValue<uint8_t>(&value));
    }
    case LogicalTypeID::DOUBLE: {
        return py::cast(RdfVariant::getValue<double>(&value));
    }
    case LogicalTypeID::FLOAT: {
        return py::cast(RdfVariant::getValue<float>(&value));
    }
    case LogicalTypeID::BOOL: {
        return py::cast(RdfVariant::getValue<bool>(&value));
    }
    case LogicalTypeID::DATE: {
        auto dateVal = RdfVariant::getValue<date_t>(&value);
        int32_t year, month, day;
        Date::convert(dateVal, year, month, day);
        return py::cast<py::object>(PyDate_FromDate(year, month, day));
    }
    case LogicalTypeID::TIMESTAMP: {
        auto timestampVal = RdfVariant::getValue<timestamp_t>(&value);
        return converTimestampToPyObject(timestampVal);
    }
    case LogicalTypeID::INTERVAL: {
        auto intervalVal = RdfVariant::getValue<interval_t>(&value);
        auto days = Interval::DAYS_PER_MONTH * intervalVal.months + intervalVal.days;
        return py::cast<py::object>(py::module::import("datetime")
                                        .attr("timedelta")(py::arg("days") = days,
                                            py::arg("microseconds") = intervalVal.micros));
    }
    default: {
        KU_UNREACHABLE;
    }
    }
}

py::object PyQueryResult::convertValueToPyObject(const Value& value) {
    if (value.isNull()) {
        return py::none();
    }
    auto dataType = value.getDataType();
    switch (dataType->getLogicalTypeID()) {
    case LogicalTypeID::BOOL: {
        return py::cast(value.getValue<bool>());
    }
    case LogicalTypeID::INT8: {
        return py::cast(value.getValue<int8_t>());
    }
    case LogicalTypeID::INT16: {
        return py::cast(value.getValue<int16_t>());
    }
    case LogicalTypeID::INT32: {
        return py::cast(value.getValue<int32_t>());
    }
    case LogicalTypeID::INT64:
    case LogicalTypeID::SERIAL: {
        return py::cast(value.getValue<int64_t>());
    }
    case LogicalTypeID::UINT8: {
        return py::cast(value.getValue<uint8_t>());
    }
    case LogicalTypeID::UINT16: {
        return py::cast(value.getValue<uint16_t>());
    }
    case LogicalTypeID::UINT32: {
        return py::cast(value.getValue<uint32_t>());
    }
    case LogicalTypeID::UINT64: {
        return py::cast(value.getValue<uint64_t>());
    }
    case LogicalTypeID::INT128: {
        kuzu::common::int128_t result = value.getValue<kuzu::common::int128_t>();
        std::string int128_string = kuzu::common::Int128_t::ToString(result);
        py::object Decimal = py::module_::import("decimal").attr("Decimal");
        py::object largeInt = Decimal(int128_string);
        return largeInt;
    }
    case LogicalTypeID::FLOAT: {
        return py::cast(value.getValue<float>());
    }
    case LogicalTypeID::DOUBLE: {
        return py::cast(value.getValue<double>());
    }
    case LogicalTypeID::STRING: {
        return py::cast(value.getValue<std::string>());
    }
    case LogicalTypeID::BLOB: {
        auto blobStr = value.getValue<std::string>();
        auto blobBytesArray = blobStr.c_str();
        return py::bytes(blobBytesArray, blobStr.size());
    }
    case LogicalTypeID::UUID: {
        kuzu::common::int128_t result = value.getValue<kuzu::common::int128_t>();
        std::string uuidString = kuzu::common::uuid_t::toString(result);
        py::object UUID = py::module_::import("uuid").attr("UUID");
        return UUID(uuidString);
    }
    case LogicalTypeID::DATE: {
        auto dateVal = value.getValue<date_t>();
        int32_t year, month, day;
        Date::convert(dateVal, year, month, day);
        return py::cast<py::object>(PyDate_FromDate(year, month, day));
    }
    case LogicalTypeID::TIMESTAMP: {
        auto timestampVal = value.getValue<timestamp_t>();
        return converTimestampToPyObject(timestampVal);
    }
    case LogicalTypeID::TIMESTAMP_TZ: {
        auto timestampVal = value.getValue<timestamp_tz_t>();
        int32_t year, month, day, hour, min, sec, micros;
        date_t date;
        dtime_t time;
        Timestamp::convert(timestampVal, date, time);
        Date::convert(date, year, month, day);
        Time::Convert(time, hour, min, sec, micros);

        return py::cast<py::object>(PyDateTimeTZ_FromDateAndTime(
            year, month, day, hour, min, sec, micros, PyDateTime_TimeZone_UTC));
    }
    case LogicalTypeID::TIMESTAMP_NS: {
        timestamp_t timestampVal =
            Timestamp::fromEpochNanoSeconds(value.getValue<timestamp_ns_t>().value);
        return converTimestampToPyObject(timestampVal);
    }
    case LogicalTypeID::TIMESTAMP_MS: {
        timestamp_t timestampVal =
            Timestamp::fromEpochMilliSeconds(value.getValue<timestamp_ms_t>().value);
        return converTimestampToPyObject(timestampVal);
    }
    case LogicalTypeID::TIMESTAMP_SEC: {
        timestamp_t timestampVal =
            Timestamp::fromEpochSeconds(value.getValue<timestamp_sec_t>().value);
        return converTimestampToPyObject(timestampVal);
    }
    case LogicalTypeID::INTERVAL: {
        auto intervalVal = value.getValue<interval_t>();
        auto days = Interval::DAYS_PER_MONTH * intervalVal.months + intervalVal.days;
        return py::cast<py::object>(py::module::import("datetime")
                                        .attr("timedelta")(py::arg("days") = days,
                                            py::arg("microseconds") = intervalVal.micros));
    }
    case LogicalTypeID::VAR_LIST:
    case LogicalTypeID::FIXED_LIST: {
        py::list list;
        for (auto i = 0u; i < NestedVal::getChildrenSize(&value); ++i) {
            list.append(convertValueToPyObject(*NestedVal::getChildVal(&value, i)));
        }
        return list;
    }
    case LogicalTypeID::MAP: {
        py::dict dict;
        for (auto i = 0u; i < NestedVal::getChildrenSize(&value); ++i) {
            auto childVal = NestedVal::getChildVal(&value, i);
            auto k = convertValueToPyObject(*NestedVal::getChildVal(childVal, 0));
            auto v = convertValueToPyObject(*NestedVal::getChildVal(childVal, 1));
            dict[std::move(k)] = std::move(v);
        }
        return dict;
    }
    case LogicalTypeID::UNION: {
        return convertValueToPyObject(*NestedVal::getChildVal(&value, 0 /* idx */));
    }
    case LogicalTypeID::STRUCT: {
        auto fieldNames = StructType::getFieldNames(dataType);
        py::dict dict;
        for (auto i = 0u; i < NestedVal::getChildrenSize(&value); ++i) {
            auto key = py::str(fieldNames[i]);
            auto val = convertValueToPyObject(*NestedVal::getChildVal(&value, i));
            dict[key] = val;
        }
        return dict;
    }
    case LogicalTypeID::RECURSIVE_REL: {
        py::dict dict;
        dict["_nodes"] = convertValueToPyObject(*NestedVal::getChildVal(&value, 0));
        dict["_rels"] = convertValueToPyObject(*NestedVal::getChildVal(&value, 1));
        return dict;
    }
    case LogicalTypeID::NODE: {
        py::dict dict;
        auto nodeIdVal = NodeVal::getNodeIDVal(&value);
        dict["_id"] = nodeIdVal ? convertValueToPyObject(*nodeIdVal) : py::none();
        auto labelVal = NodeVal::getLabelVal(&value);
        dict["_label"] = labelVal ? convertValueToPyObject(*labelVal) : py::none();
        auto numProperties = NodeVal::getNumProperties(&value);
        for (auto i = 0u; i < numProperties; ++i) {
            auto key = py::str(NodeVal::getPropertyName(&value, i));
            auto val = convertValueToPyObject(*NodeVal::getPropertyVal(&value, i));
            dict[key] = val;
        }
        return dict;
    }
    case LogicalTypeID::REL: {
        py::dict dict;
        auto srcIdVal = RelVal::getSrcNodeIDVal(&value);
        dict["_src"] = srcIdVal ? convertValueToPyObject(*srcIdVal) : py::none();
        auto dstIdVal = RelVal::getDstNodeIDVal(&value);
        dict["_dst"] = dstIdVal ? convertValueToPyObject(*dstIdVal) : py::none();
        auto labelVal = RelVal::getLabelVal(&value);
        dict["_label"] = labelVal ? convertValueToPyObject(*labelVal) : py::none();
        auto numProperties = RelVal::getNumProperties(&value);
        for (auto i = 0u; i < numProperties; ++i) {
            auto key = py::str(RelVal::getPropertyName(&value, i));
            auto val = convertValueToPyObject(*RelVal::getPropertyVal(&value, i));
            dict[key] = val;
        }
        return dict;
    }
    case LogicalTypeID::INTERNAL_ID: {
        return convertNodeIdToPyDict(value.getValue<nodeID_t>());
    }
    case LogicalTypeID::RDF_VARIANT: {
        return convertRdfVariantToPyObject(value);
    }

    default:
        throw NotImplementedException("Unsupported type: " + dataType->toString());
    }
}

py::object PyQueryResult::getAsDF() {
    return QueryResultConverter(queryResult.get()).toDF();
}

bool PyQueryResult::getNextArrowChunk(const std::vector<std::unique_ptr<DataTypeInfo>>& typesInfo,
    py::list& batches, std::int64_t chunkSize) {
    if (!queryResult->hasNext()) {
        return false;
    }
    ArrowArray data;
    ArrowConverter::toArrowArray(*queryResult, &data, chunkSize);

    // TODO(Ziyi): use import cache to improve performance.
    auto pyarrowLibModule = py::module::import("pyarrow").attr("lib");
    auto batchImportFunc = pyarrowLibModule.attr("RecordBatch").attr("_import_from_c");
    auto schema = ArrowConverter::toArrowSchema(typesInfo);
    batches.append(batchImportFunc((std::uint64_t)&data, (std::uint64_t)schema.get()));
    return true;
}

py::object PyQueryResult::getArrowChunks(
    const std::vector<std::unique_ptr<DataTypeInfo>>& typesInfo, std::int64_t chunkSize) {
    auto pyarrowLibModule = py::module::import("pyarrow").attr("lib");
    py::list batches;
    while (getNextArrowChunk(typesInfo, batches, chunkSize)) {}
    return batches;
}

kuzu::pyarrow::Table PyQueryResult::getAsArrow(std::int64_t chunkSize) {
    py::gil_scoped_acquire acquire;

    auto pyarrowLibModule = py::module::import("pyarrow").attr("lib");
    auto fromBatchesFunc = pyarrowLibModule.attr("Table").attr("from_batches");
    auto schemaImportFunc = pyarrowLibModule.attr("Schema").attr("_import_from_c");

    auto typesInfo = queryResult->getColumnTypesInfo();
    py::list batches = getArrowChunks(typesInfo, chunkSize);
    auto schema = ArrowConverter::toArrowSchema(typesInfo);
    auto schemaObj = schemaImportFunc((std::uint64_t)schema.get());
    return py::cast<kuzu::pyarrow::Table>(fromBatchesFunc(batches, schemaObj));
}

py::list PyQueryResult::getColumnDataTypes() {
    auto columnDataTypes = queryResult->getColumnDataTypes();
    py::tuple result(columnDataTypes.size());
    for (auto i = 0u; i < columnDataTypes.size(); ++i) {
        result[i] = py::cast(columnDataTypes[i].toString());
    }
    return result;
}

py::list PyQueryResult::getColumnNames() {
    auto columnNames = queryResult->getColumnNames();
    py::tuple result(columnNames.size());
    for (auto i = 0u; i < columnNames.size(); ++i) {
        result[i] = py::cast(columnNames[i]);
    }
    return result;
}

void PyQueryResult::resetIterator() {
    queryResult->resetIterator();
}

bool PyQueryResult::isSuccess() const {
    return queryResult->isSuccess();
}

std::string PyQueryResult::getErrorMessage() const {
    return queryResult->getErrorMessage();
}

py::dict PyQueryResult::convertNodeIdToPyDict(const nodeID_t& nodeId) {
    py::dict idDict;
    idDict["offset"] = py::cast(nodeId.offset);
    idDict["table"] = py::cast(nodeId.tableID);
    return idDict;
}

double PyQueryResult::getExecutionTime() {
    return queryResult->getQuerySummary()->getExecutionTime();
}

double PyQueryResult::getCompilingTime() {
    return queryResult->getQuerySummary()->getCompilingTime();
}

size_t PyQueryResult::getNumTuples() {
    return queryResult->getNumTuples();
}
