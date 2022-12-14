#include "include/py_query_result.h"

#include <string>

#include "datetime.h" // python lib
#include "include/py_query_result_converter.h"

using namespace kuzu::common;

PyQueryResult::PyQueryResult() {
    auto atexit = py::module_::import("atexit");
    atexit.attr("register")(py::cpp_function([&]() {
        if (queryResult) {
            queryResult.reset();
        }
    }));
}

void PyQueryResult::initialize(py::handle& m) {
    py::class_<PyQueryResult>(m, "result")
        .def("hasNext", &PyQueryResult::hasNext)
        .def("getNext", &PyQueryResult::getNext)
        .def("writeToCSV", &PyQueryResult::writeToCSV, py::arg("filename"),
            py::arg("delimiter") = ",", py::arg("escapeCharacter") = "\"",
            py::arg("newline") = "\n")
        .def("close", &PyQueryResult::close)
        .def("getAsDF", &PyQueryResult::getAsDF)
        .def("getColumnNames", &PyQueryResult::getColumnNames)
        .def("getColumnDataTypes", &PyQueryResult::getColumnDataTypes);
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
        result[i] = convertValueToPyObject(*tuple->getResultValue(i));
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

py::object PyQueryResult::convertValueToPyObject(const ResultValue& value) {
    if (value.isNullVal()) {
        return py::none();
    }
    auto dataType = value.getDataType();
    switch (dataType.typeID) {
    case BOOL: {
        return py::cast(value.getBooleanVal());
    }
    case INT64: {
        return py::cast(value.getInt64Val());
    }
    case DOUBLE: {
        return py::cast(value.getDoubleVal());
    }
    case STRING: {
        return py::cast(value.getStringVal());
    }
    case DATE: {
        auto dateVal = value.getDateVal();
        int32_t year, month, day;
        Date::Convert(dateVal, year, month, day);
        return py::cast<py::object>(PyDate_FromDate(year, month, day));
    }
    case TIMESTAMP: {
        auto timestampVal = value.getTimestampVal();
        int32_t year, month, day, hour, min, sec, micros;
        date_t date;
        dtime_t time;
        Timestamp::Convert(timestampVal, date, time);
        Date::Convert(date, year, month, day);
        Time::Convert(time, hour, min, sec, micros);
        return py::cast<py::object>(
            PyDateTime_FromDateAndTime(year, month, day, hour, min, sec, micros));
    }
    case INTERVAL: {
        auto intervalVal = value.getIntervalVal();
        auto days = Interval::DAYS_PER_MONTH * intervalVal.months + intervalVal.days;
        return py::cast<py::object>(py::module::import("datetime")
                                        .attr("timedelta")(py::arg("days") = days,
                                            py::arg("microseconds") = intervalVal.micros));
    }
    case LIST: {
        auto listVal = value.getListVal();
        py::list list;
        for (auto i = 0u; i < listVal.size(); ++i) {
            list.append(convertValueToPyObject(listVal[i]));
        }
        return std::move(list);
    }
    default:
        throw NotImplementedException("Unsupported type2: " + Types::dataTypeToString(dataType));
    }
}

py::object PyQueryResult::getAsDF() {
    return QueryResultConverter(queryResult.get()).toDF();
}

py::list PyQueryResult::getColumnDataTypes() {
    auto columnDataTypes = queryResult->getColumnDataTypes();
    py::tuple result(columnDataTypes.size());
    for (auto i = 0u; i < columnDataTypes.size(); ++i) {
        result[i] = py::cast(Types::dataTypeToString(columnDataTypes[i]));
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
