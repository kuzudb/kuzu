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

    void close();

    static py::object convertValueToPyObject(const Value& value, bool isNull);

    py::object getAsDF();

private:
    static py::object convertValueToPyObject(uint8_t* val, const DataType& dataType);

private:
    unique_ptr<QueryResult> queryResult;
};
