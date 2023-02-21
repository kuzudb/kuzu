#pragma once

#include "main/kuzu.h"
#include "main/prepared_statement.h"
#include "pybind_include.h"

using namespace kuzu::main;

class PyPreparedStatement {
    friend class PyConnection;

public:
    static void initialize(py::handle& m);

    py::str getErrorMessage() const;

    bool isSuccess() const;

private:
    std::unique_ptr<PreparedStatement> preparedStatement;
};
