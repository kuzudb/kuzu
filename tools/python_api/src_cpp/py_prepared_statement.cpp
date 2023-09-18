#include "include/py_prepared_statement.h"

#include "binder/binder.h"
#include "planner/operator/logical_plan.h"

void PyPreparedStatement::initialize(py::handle& m) {
    py::class_<PyPreparedStatement>(m, "PreparedStatement")
        .def("get_error_message", &PyPreparedStatement::getErrorMessage)
        .def("is_success", &PyPreparedStatement::isSuccess);
}

py::str PyPreparedStatement::getErrorMessage() const {
    return preparedStatement->getErrorMessage();
}

bool PyPreparedStatement::isSuccess() const {
    return preparedStatement->isSuccess();
}
