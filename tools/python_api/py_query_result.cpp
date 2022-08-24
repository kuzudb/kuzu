#include "include/py_query_result.h"

#include <fstream>

#include "datetime.h" // python lib
#include "include/py_query_result_converter.h"

using namespace graphflow::common;

void PyQueryResult::initialize(py::handle& m) {
    py::class_<PyQueryResult>(m, "result")
        .def("hasNext", &PyQueryResult::hasNext)
        .def("getNext", &PyQueryResult::getNext)
        .def("close", &PyQueryResult::close)
        .def("getAsDF", &PyQueryResult::getAsDF);

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
        result[i] = convertValueToPyObject(*tuple->getValue(i), tuple->nullMask[i]);
    }
    return move(result);
}

void PyQueryResult::close() {
    // Note: Python does not guarantee objects to be deleted in the reverse order. Therefore, we
    // expose close() interface so that users can explicitly call close() and ensure that
    // QueryResult is destroyed before Database.
    queryResult.reset();
}

py::object PyQueryResult::convertValueToPyObject(const Value& value, bool isNull) {
    if (isNull) {
        return py::none();
    }
    auto dataType = value.dataType;
    switch (dataType.typeID) {
    case BOOL: {
        return convertValueToPyObject((uint8_t*)(&value.val.booleanVal), dataType);
    }
    case INT64: {
        return convertValueToPyObject((uint8_t*)(&value.val.int64Val), dataType);
    }
    case DOUBLE: {
        return convertValueToPyObject((uint8_t*)(&value.val.doubleVal), dataType);
    }
    case STRING: {
        return convertValueToPyObject((uint8_t*)(&value.val.strVal), dataType);
    }
    case DATE: {
        return convertValueToPyObject((uint8_t*)(&value.val.dateVal), dataType);
    }
    case TIMESTAMP: {
        return convertValueToPyObject((uint8_t*)(&value.val.timestampVal), dataType);
    }
    case INTERVAL: {
        return convertValueToPyObject((uint8_t*)(&value.val.intervalVal), dataType);
    }
    case LIST: {
        return convertValueToPyObject((uint8_t*)(&value.val.listVal), dataType);
    }
    case NODE_ID: {
        // TODO: Somehow Neo4j allows exposing of internal type. But I start thinking we shouldn't.
        return convertValueToPyObject((uint8_t*)(&value.val.nodeID), dataType);
    }
    default:
        throw NotImplementedException(
            "Unsupported type1: " + Types::dataTypeToString(value.dataType));
    }
}

py::object PyQueryResult::convertValueToPyObject(uint8_t* val, const DataType& dataType) {
    switch (dataType.typeID) {
    case BOOL: {
        return py::cast(*(bool*)val);
    }
    case INT64: {
        return py::cast(*(int64_t*)val);
    }
    case DOUBLE: {
        return py::cast(*(double_t*)val);
    }
    case STRING: {
        return py::cast((*(gf_string_t*)val).getAsString());
    }
    case DATE: {
        auto dateVal = *(date_t*)val;
        int32_t year, month, day;
        Date::Convert(dateVal, year, month, day);
        return py::cast<py::object>(PyDate_FromDate(year, month, day));
    }
    case TIMESTAMP: {
        auto timestampVal = *(timestamp_t*)val;
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
        auto intervalVal = *(interval_t*)val;
        auto days = Interval::DAYS_PER_MONTH * intervalVal.months + intervalVal.days;
        return py::cast<py::object>(py::module::import("datetime")
                                        .attr("timedelta")(py::arg("days") = days,
                                            py::arg("microseconds") = intervalVal.micros));
    }
    case LIST: {
        auto listVal = *(gf_list_t*)val;
        auto childTypeSize = Types::getDataTypeSize(*dataType.childType);
        py::list list;
        for (auto i = 0u; i < listVal.size; ++i) {
            list.append(convertValueToPyObject(
                (uint8_t*)(listVal.overflowPtr + childTypeSize * i), *dataType.childType));
        }
        return move(list);
    }
    case NODE_ID: {
        auto nodeVal = *(nodeID_t*)val;
        return py::cast(make_pair(nodeVal.tableID, nodeVal.offset));
    }
    default:
        throw NotImplementedException("Unsupported type2: " + Types::dataTypeToString(dataType));
    }
}

py::object PyQueryResult::getAsDF() {
    return QueryResultConverter(queryResult.get()).toDF();
}
