#include "include/py_connection.h"

#include "binder/bound_statement_result.h"
#include "datetime.h" // from Python
#include "json.hpp"
#include "main/connection.h"
#include "planner/logical_plan/logical_plan.h"
#include "processor/result/factorized_table.h"

using namespace kuzu::common;

void PyConnection::initialize(py::handle& m) {
    py::class_<PyConnection>(m, "Connection")
        .def(py::init<PyDatabase*, uint64_t>(), py::arg("database"), py::arg("num_threads") = 0)
        .def(
            "execute", &PyConnection::execute, py::arg("query"), py::arg("parameters") = py::list())
        .def("set_max_threads_for_exec", &PyConnection::setMaxNumThreadForExec,
            py::arg("num_threads"))
        .def("get_node_property_names", &PyConnection::getNodePropertyNames, py::arg("table_name"));
    PyDateTime_IMPORT;
}

PyConnection::PyConnection(PyDatabase* pyDatabase, uint64_t numThreads) {
    conn = std::make_unique<Connection>(pyDatabase->database.get());
    if (numThreads > 0) {
        conn->setMaxNumThreadForExec(numThreads);
    }
}

std::unique_ptr<PyQueryResult> PyConnection::execute(const std::string& query, py::list params) {
    auto preparedStatement = conn->prepare(query);
    auto parameters = transformPythonParameters(params);
    py::gil_scoped_release release;
    auto queryResult = conn->executeWithParams(preparedStatement.get(), parameters);
    py::gil_scoped_acquire acquire;
    if (!queryResult->isSuccess()) {
        throw std::runtime_error(queryResult->getErrorMessage());
    }
    auto pyQueryResult = std::make_unique<PyQueryResult>();
    pyQueryResult->queryResult = std::move(queryResult);
    return pyQueryResult;
}

void PyConnection::setMaxNumThreadForExec(uint64_t numThreads) {
    conn->setMaxNumThreadForExec(numThreads);
}

py::str PyConnection::getNodePropertyNames(const std::string& tableName) {
    return conn->getNodePropertyNames(tableName);
}

std::unordered_map<std::string, std::shared_ptr<Value>> PyConnection::transformPythonParameters(
    py::list params) {
    std::unordered_map<std::string, std::shared_ptr<Value>> result;
    for (auto param : params) {
        if (!py::isinstance<py::tuple>(param)) {
            throw std::runtime_error("Each parameter must be in the form of <name, val>");
        }
        auto [name, val] = transformPythonParameter(param.cast<py::tuple>());
        result.insert({name, val});
    }
    return result;
}

std::pair<std::string, std::shared_ptr<Value>> PyConnection::transformPythonParameter(
    py::tuple param) {
    if (py::len(param) != 2) {
        throw std::runtime_error("Each parameter must be in the form of <name, val>");
    }
    if (!py::isinstance<py::str>(param[0])) {
        throw std::runtime_error("Parameter name must be of type string but get " +
                                 py::str(param[0].get_type()).cast<std::string>());
    }
    auto val = transformPythonValue(param[1]);
    return make_pair(param[0].cast<std::string>(), std::make_shared<Value>(val));
}

Value PyConnection::transformPythonValue(py::handle val) {
    auto datetime_mod = py::module::import("datetime");
    auto datetime_datetime = datetime_mod.attr("datetime");
    auto datetime_time = datetime_mod.attr("time");
    auto datetime_date = datetime_mod.attr("date");
    if (py::isinstance<py::bool_>(val)) {
        return Value::createValue<bool>(val.cast<bool>());
    } else if (py::isinstance<py::int_>(val)) {
        return Value::createValue<int64_t>(val.cast<int64_t>());
    } else if (py::isinstance<py::float_>(val)) {
        return Value::createValue<double_t>(val.cast<double_t>());
    } else if (py::isinstance<py::str>(val)) {
        return Value::createValue<std::string>(val.cast<std::string>());
    } else if (py::isinstance(val, datetime_datetime)) {
        auto ptr = val.ptr();
        auto year = PyDateTime_GET_YEAR(ptr);
        auto month = PyDateTime_GET_MONTH(ptr);
        auto day = PyDateTime_GET_DAY(ptr);
        auto hour = PyDateTime_DATE_GET_HOUR(ptr);
        auto minute = PyDateTime_DATE_GET_MINUTE(ptr);
        auto second = PyDateTime_DATE_GET_SECOND(ptr);
        auto micros = PyDateTime_DATE_GET_MICROSECOND(ptr);
        auto date = Date::FromDate(year, month, day);
        auto time = Time::FromTime(hour, minute, second, micros);
        return Value::createValue<timestamp_t>(Timestamp::FromDatetime(date, time));
    } else if (py::isinstance(val, datetime_date)) {
        auto ptr = val.ptr();
        auto year = PyDateTime_GET_YEAR(ptr);
        auto month = PyDateTime_GET_MONTH(ptr);
        auto day = PyDateTime_GET_DAY(ptr);
        return Value::createValue<date_t>(Date::FromDate(year, month, day));
    } else {
        throw std::runtime_error(
            "Unknown parameter type " + py::str(val.get_type()).cast<std::string>());
    }
}
