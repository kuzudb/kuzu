#include "include/py_connection.h"

#include "datetime.h" // from Python

void PyConnection::initialize(py::handle& m) {
    py::class_<PyConnection>(m, "connection")
        .def(py::init<PyDatabase*>())
        .def("execute", &PyConnection::execute, py::arg("query"),
            py::arg("parameters") = py::list());

    PyDateTime_IMPORT;
}

PyConnection::PyConnection(PyDatabase* pyDatabase) {
    conn = make_unique<Connection>(pyDatabase->database.get());
}

unique_ptr<PyQueryResult> PyConnection::execute(const string& query, py::list params) {
    auto preparedStatement = conn->prepare(query);
    auto parameters = transformPythonParameters(params);
    auto queryResult = conn->executeWithParams(preparedStatement.get(), parameters);
    if (!queryResult->isSuccess()) {
        throw runtime_error(queryResult->getErrorMessage());
    }
    auto pyQueryResult = make_unique<PyQueryResult>();
    pyQueryResult->queryResult = move(queryResult);
    return pyQueryResult;
}

unordered_map<string, shared_ptr<Literal>> PyConnection::transformPythonParameters(
    py::list params) {
    unordered_map<string, shared_ptr<Literal>> result;
    for (auto param : params) {
        if (!py::isinstance<py::tuple>(param)) {
            throw runtime_error("Each parameter must be in the form of <name, val>");
        }
        auto [name, val] = transformPythonParameter(param.cast<py::tuple>());
        result.insert({name, val});
    }
    return result;
}

pair<string, shared_ptr<Literal>> PyConnection::transformPythonParameter(py::tuple param) {
    if (py::len(param) != 2) {
        throw runtime_error("Each parameter must be in the form of <name, val>");
    }
    if (!py::isinstance<py::str>(param[0])) {
        throw runtime_error("Parameter name must be of type string but get " +
                            py::str(param[0].get_type()).cast<string>());
    }
    auto val = transformPythonValue(param[1]);
    return make_pair(param[0].cast<string>(), make_shared<Literal>(val));
}

Literal PyConnection::transformPythonValue(py::handle val) {
    auto datetime_mod = py::module::import("datetime");
    auto datetime_datetime = datetime_mod.attr("datetime");
    auto datetime_time = datetime_mod.attr("time");
    auto datetime_date = datetime_mod.attr("date");
    if (py::isinstance<py::bool_>(val)) {
        return Literal::createLiteral(val.cast<bool>());
    } else if (py::isinstance<py::int_>(val)) {
        return Literal::createLiteral(val.cast<int64_t>());
    } else if (py::isinstance<py::float_>(val)) {
        return Literal::createLiteral(val.cast<double_t>());
    } else if (py::isinstance<py::str>(val)) {
        return Literal::createLiteral(val.cast<string>());
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
        return Literal::createLiteral(Timestamp::FromDatetime(date, time));
    } else if (py::isinstance(val, datetime_date)) {
        auto ptr = val.ptr();
        auto year = PyDateTime_GET_YEAR(ptr);
        auto month = PyDateTime_GET_MONTH(ptr);
        auto day = PyDateTime_GET_DAY(ptr);
        return Literal::createLiteral(Date::FromDate(year, month, day));
    } else {
        throw runtime_error("Unknown parameter type " + py::str(val.get_type()).cast<string>());
    }
}
