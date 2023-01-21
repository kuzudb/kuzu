#pragma once

#include "arrow_array.h"
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

    void writeToCSV(const py::str& filename, const py::str& delimiter,
        const py::str& escapeCharacter, const py::str& newline);

    void close();

    static py::object convertValueToPyObject(const Value& value);

    py::object getAsDF();

    kuzu::pyarrow::Table getAsArrow(std::int64_t chunkSize);

    py::list getColumnDataTypes();

    py::list getColumnNames();

    void resetIterator();

private:
    static py::dict getPyDictFromProperties(
        const vector<pair<std::string, unique_ptr<Value>>>& properties);

    static py::dict convertNodeIdToPyDict(const nodeID_t& nodeId);

    bool getNextArrowChunk(py::list& batches, std::int64_t chunk_size);
    py::object getArrowChunks(std::int64_t chunkSize);

private:
    unique_ptr<QueryResult> queryResult;
};
