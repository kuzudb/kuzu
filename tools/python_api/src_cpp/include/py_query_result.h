#pragma once

#include "arrow_array.h"
#include "common/arrow/arrow.h"
#include "common/types/internal_id_t.h"
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

    static py::object convertValueToPyObject(const kuzu::common::Value& value);

    py::object getAsDF();

    kuzu::pyarrow::Table getAsArrow(std::int64_t chunkSize);

    py::list getColumnDataTypes();

    py::list getColumnNames();

    void resetIterator();

    bool isSuccess() const;

    double getExecutionTime();

    double getCompilingTime();

    size_t getNumTuples();

private:
    static py::dict getPyDictFromProperties(
        const std::vector<std::pair<std::string, std::unique_ptr<kuzu::common::Value>>>&
            properties);

    static py::dict convertNodeIdToPyDict(const kuzu::common::nodeID_t& nodeId);

    bool getNextArrowChunk(const ArrowSchema& schema, py::list& batches, std::int64_t chunk_size);
    py::object getArrowChunks(const ArrowSchema& schema, std::int64_t chunkSize);

private:
    std::unique_ptr<QueryResult> queryResult;
};
