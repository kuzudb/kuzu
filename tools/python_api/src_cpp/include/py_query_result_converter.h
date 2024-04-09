#pragma once

#include "main/kuzu.h"
#include "pybind_include.h"

using kuzu::common::LogicalType;
using kuzu::common::Value;

struct NPArrayWrapper {

public:
    NPArrayWrapper(const LogicalType& type, uint64_t numFlatTuple);

    void appendElement(Value* value);

private:
    py::dtype convertToArrayType(const LogicalType& type);

public:
    py::array data;
    uint8_t* dataBuffer;
    py::array mask;
    LogicalType type;
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
