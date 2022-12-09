#pragma once

#include "main/kuzu.h"
#include "pybind_include.h"

using namespace kuzu::main;

class PyQueryResult {
    friend class PyConnection;

public:
    PyQueryResult();
    ~PyQueryResult() = default;

    static void initialize(py::handle& m);

    bool hasNext();

    py::list getNext();

    void writeToCSV(const py::str& filename, const py::str& delimiter,
        const py::str& escapeCharacter, const py::str& newline);

    void close();

    static py::object convertValueToPyObject(const ResultValue& value);

    py::object getAsDF();

    py::list getColumnDataTypes();

    py::list getColumnNames();

private:
    unique_ptr<QueryResult> queryResult;
};
