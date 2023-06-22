#include "include/py_query_result.h"

#include <string>

#include "common/arrow/arrow_converter.h"
#include "datetime.h" // python lib
#include "include/py_query_result_converter.h"
#include "json.hpp"
#include "processor/result/factorized_table.h"
#include "processor/result/flat_tuple.h"

using namespace kuzu::common;

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
    return std::move(result);
}

void PyQueryResult::writeToCSV(const py::str& filename, const py::str& delimiter,
    const py::str& escapeCharacter, const py::str& newline) {
    std::string delimiterStr = delimiter;
    std::string escapeCharacterStr = escapeCharacter;
    std::string newlineStr = newline;
    assert(delimiterStr.size() == 1);
    assert(escapeCharacterStr.size() == 1);
    assert(newlineStr.size() == 1);
    queryResult->writeToCSV(filename, delimiterStr[0], escapeCharacterStr[0], newlineStr[0]);
}

void PyQueryResult::close() {
    // Note: Python does not guarantee objects to be deleted in the reverse order. Therefore, we
    // expose close() interface so that users can explicitly call close() and ensure that
    // QueryResult is destroyed before Database.
    queryResult.reset();
}

py::object PyQueryResult::convertValueToPyObject(const Value& value) {
    if (value.isNull()) {
        return py::none();
    }
    auto dataType = value.getDataType();
    switch (dataType.getLogicalTypeID()) {
    case LogicalTypeID::BOOL: {
        return py::cast(value.getValue<bool>());
    }
    case LogicalTypeID::INT16: {
        return py::cast(value.getValue<int16_t>());
    }
    case LogicalTypeID::INT32: {
        return py::cast(value.getValue<int32_t>());
    }
    case LogicalTypeID::INT64: {
        return py::cast(value.getValue<int64_t>());
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
    case LogicalTypeID::DATE: {
        auto dateVal = value.getValue<date_t>();
        int32_t year, month, day;
        Date::Convert(dateVal, year, month, day);
        return py::cast<py::object>(PyDate_FromDate(year, month, day));
    }
    case LogicalTypeID::TIMESTAMP: {
        auto timestampVal = value.getValue<timestamp_t>();
        int32_t year, month, day, hour, min, sec, micros;
        date_t date;
        dtime_t time;
        Timestamp::Convert(timestampVal, date, time);
        Date::Convert(date, year, month, day);
        Time::Convert(time, hour, min, sec, micros);
        return py::cast<py::object>(
            PyDateTime_FromDateAndTime(year, month, day, hour, min, sec, micros));
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
        auto& listVal = value.getListValReference();
        py::list list;
        for (auto i = 0u; i < listVal.size(); ++i) {
            list.append(convertValueToPyObject(*listVal[i]));
        }
        return std::move(list);
    }
    case LogicalTypeID::STRUCT: {
        auto fieldNames = StructType::getFieldNames(&dataType);
        py::dict dict;
        auto& structVals = value.getListValReference();
        for (auto i = 0u; i < structVals.size(); ++i) {
            auto key = py::str(fieldNames[i]);
            auto val = convertValueToPyObject(*structVals[i]);
            dict[key] = val;
        }
        return dict;
    }
    case LogicalTypeID::NODE: {
        auto nodeVal = value.getValue<NodeVal>();
        auto dict = PyQueryResult::getPyDictFromProperties(nodeVal.getProperties());
        dict["_label"] = py::cast(nodeVal.getLabelName());
        dict["_id"] = convertNodeIdToPyDict(nodeVal.getNodeID());
        return std::move(dict);
    }
    case LogicalTypeID::REL: {
        auto relVal = value.getValue<RelVal>();
        auto dict = PyQueryResult::getPyDictFromProperties(relVal.getProperties());
        dict["_src"] = convertNodeIdToPyDict(relVal.getSrcNodeID());
        dict["_dst"] = convertNodeIdToPyDict(relVal.getDstNodeID());
        return std::move(dict);
    }
    case LogicalTypeID::INTERNAL_ID: {
        return convertNodeIdToPyDict(value.getValue<nodeID_t>());
    }
    default:
        throw NotImplementedException("Unsupported type: " + LogicalTypeUtils::dataTypeToString(dataType));
    }
}

py::object PyQueryResult::getAsDF() {
    return QueryResultConverter(queryResult.get()).toDF();
}

bool PyQueryResult::getNextArrowChunk(
    const ArrowSchema& schema, py::list& batches, std::int64_t chunkSize) {
    if (!queryResult->hasNext()) {
        return false;
    }
    ArrowArray data;
    ArrowConverter::toArrowArray(*queryResult, &data, chunkSize);

    auto pyarrowLibModule = py::module::import("pyarrow").attr("lib");
    auto batchImportFunc = pyarrowLibModule.attr("RecordBatch").attr("_import_from_c");
    batches.append(batchImportFunc((std::uint64_t)&data, (std::uint64_t)&schema));
    return true;
}

py::object PyQueryResult::getArrowChunks(const ArrowSchema& schema, std::int64_t chunkSize) {
    auto pyarrowLibModule = py::module::import("pyarrow").attr("lib");
    py::list batches;
    while (getNextArrowChunk(schema, batches, chunkSize)) {}
    return std::move(batches);
}

kuzu::pyarrow::Table PyQueryResult::getAsArrow(std::int64_t chunkSize) {
    py::gil_scoped_acquire acquire;

    auto pyarrowLibModule = py::module::import("pyarrow").attr("lib");
    auto fromBatchesFunc = pyarrowLibModule.attr("Table").attr("from_batches");
    auto schemaImportFunc = pyarrowLibModule.attr("Schema").attr("_import_from_c");

    auto typesInfo = queryResult->getColumnTypesInfo();
    auto schema = ArrowConverter::toArrowSchema(typesInfo);
    auto schemaObj = schemaImportFunc((std::uint64_t)schema.get());
    py::list batches = getArrowChunks(*schema, chunkSize);
    return py::cast<kuzu::pyarrow::Table>(fromBatchesFunc(batches, schemaObj));
}

py::list PyQueryResult::getColumnDataTypes() {
    auto columnDataTypes = queryResult->getColumnDataTypes();
    py::tuple result(columnDataTypes.size());
    for (auto i = 0u; i < columnDataTypes.size(); ++i) {
        result[i] = py::cast(LogicalTypeUtils::dataTypeToString(columnDataTypes[i]));
    }
    return std::move(result);
}

py::list PyQueryResult::getColumnNames() {
    auto columnNames = queryResult->getColumnNames();
    py::tuple result(columnNames.size());
    for (auto i = 0u; i < columnNames.size(); ++i) {
        result[i] = py::cast(columnNames[i]);
    }
    return std::move(result);
}

void PyQueryResult::resetIterator() {
    queryResult->resetIterator();
}

bool PyQueryResult::isSuccess() const {
    return queryResult->isSuccess();
}

py::dict PyQueryResult::getPyDictFromProperties(
    const std::vector<std::pair<std::string, std::unique_ptr<Value>>>& properties) {
    py::dict result;
    for (auto i = 0u; i < properties.size(); ++i) {
        auto& [name, value] = properties[i];
        result[py::cast(name)] = convertValueToPyObject(*value);
    }
    return result;
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
