#pragma once

#include "pybind_include.h"

#include "src/main/include/graphflowdb.h"

using namespace graphflow::main;

struct NPArrayWrapper {

public:
    NPArrayWrapper(const DataType& type, uint64_t numFlatTuple);

    void appendElement(Value* value, bool isNull);

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
