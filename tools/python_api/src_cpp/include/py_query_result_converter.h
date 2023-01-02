#pragma once

#include "main/kuzu.h"
#include "pybind_include.h"

using namespace kuzu::main;

struct NPArrayWrapper {

public:
    NPArrayWrapper(const DataType& type, uint64_t numFlatTuple);

    void appendElement(ResultValue* value);

private:
    py::dtype convertToArrayType(const DataType& type);

public:
    py::array data;
    uint8_t* dataBuffer;
    py::array mask;
    DataType type;
    uint64_t numElements;
};

class QueryResultConverter {
public:
    explicit QueryResultConverter(QueryResult* queryResult);

    py::object toDF();

private:
    QueryResult* queryResult;
    vector<unique_ptr<NPArrayWrapper>> columns;
};
