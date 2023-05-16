#pragma once

#include "main/kuzu.h"
#include "pybind_include.h"

struct NPArrayWrapper {

public:
    NPArrayWrapper(const kuzu::common::LogicalType& type, uint64_t numFlatTuple);

    void appendElement(kuzu::common::Value* value);

private:
    py::dtype convertToArrayType(const kuzu::common::LogicalType& type);

public:
    py::array data;
    uint8_t* dataBuffer;
    py::array mask;
    kuzu::common::LogicalType type;
    uint64_t numElements;
};

class QueryResultConverter {
public:
    explicit QueryResultConverter(kuzu::main::QueryResult* queryResult);

    py::object toDF();

private:
    kuzu::main::QueryResult* queryResult;
    std::vector<std::unique_ptr<NPArrayWrapper>> columns;
};
