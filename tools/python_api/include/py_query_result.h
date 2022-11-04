#pragma once

#include "pybind_include.h"

#include "src/main/include/graphflowdb.h"

using namespace graphflow::main;

class PyQueryResult {
    friend class PyConnection;

public:
    static void initialize(py::handle& m);

    PyQueryResult() = default;
    ~PyQueryResult() = default;

    bool hasNext();

    py::list getNext();

    void writeToCSV(py::str filename);

    void close();

    static py::object convertValueToPyObject(const ResultValue& value);

    py::object getAsDF();

    py::list getColumnDataTypes();

    py::list getColumnNames();

private:
    unique_ptr<QueryResult> queryResult;
};
