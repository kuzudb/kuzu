#pragma once

#include <memory>
#include <vector>

#include "arrow_array.h"
#include "common/arrow/arrow.h"
#include "main/kuzu.h"
#include "pybind_include.h"

using namespace kuzu::main;

class PyQueryResult {
    friend class PyConnection;

public:
    static void initialize(py::handle& m);

    PyQueryResult() = default;

    ~PyQueryResult();

    bool hasNext();

    py::list getNext();

    bool hasNextQueryResult();

    std::unique_ptr<PyQueryResult> getNextQueryResult();

    void close();

    static py::object convertValueToPyObject(const kuzu::common::Value& value);

    py::object getAsDF();

    kuzu::pyarrow::Table getAsArrow(std::int64_t chunkSize);

    py::list getColumnDataTypes();

    py::list getColumnNames();

    void resetIterator();

    bool isSuccess() const;

    std::string getErrorMessage() const;

    double getExecutionTime();

    double getCompilingTime();

    size_t getNumTuples();

private:
    static py::dict convertNodeIdToPyDict(const kuzu::common::nodeID_t& nodeId);

    bool getNextArrowChunk(const std::vector<kuzu::common::LogicalType>& types,
        const std::vector<std::string>& names, py::list& batches, std::int64_t chunk_size);
    py::object getArrowChunks(const std::vector<kuzu::common::LogicalType>& types,
        const std::vector<std::string>& names, std::int64_t chunkSize);

private:
    QueryResult* queryResult = nullptr;
    bool isOwned = false;
};
