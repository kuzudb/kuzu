#include "include/py_connection.h"

void PyConnection::initialize(py::handle& m) {
    py::class_<PyConnection>(m, "connection")
        .def(py::init<PyDatabase*>())
        .def("query", &PyConnection::query);
}

PyConnection::PyConnection(PyDatabase* pyDatabase) {
    conn = make_unique<Connection>(pyDatabase->database.get());
}

unique_ptr<PyQueryResult> PyConnection::query(const string& query) {
    auto queryResult = conn->query(query);
    auto pyQueryResult = make_unique<PyQueryResult>();
    pyQueryResult->queryResult = move(queryResult);
    return pyQueryResult;
}
