#pragma once

#include "main/kuzu.h"
#include "pybind_include.h"

using namespace kuzu::main;

class PyQueryResult {
    friend class PyConnection;

public:
    static void initialize(py::handle& m);

    PyQueryResult() = default;
    ~PyQueryResult() = default;

    bool hasNext();

    py::list getNext();

    void writeToCSV(py::str filename, py::str delimiter, py::str escapeCharacter, py::str newline);

    void close();

    static py::object convertValueToPyObject(const Value& value);

    py::object getAsDF();

    py::list getColumnDataTypes();

    py::list getColumnNames();

    void resetIterator();

private:
    static py::dict getPyDictFromProperties(
        const vector<pair<std::string, unique_ptr<Value>>>& properties);

    static py::dict convertNodeIdToPyDict(const nodeID_t& nodeId);

    unique_ptr<QueryResult> queryResult;
};
