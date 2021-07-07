#pragma once

#include <memory>

#include "src/common/include/types.h"

namespace graphflow {
namespace common {

const string COUNT_STAR_FUNC_NAME = "COUNT_STAR";
const string ID_FUNC_NAME = "ID";
const string DATE_FUNC_NAME = "DATE";

class Function {

public:
    inline uint32_t getNumParameters() const { return parameterTypes.size(); }

protected:
    Function(string name, DataType returnType) : name{move(name)}, returnType{returnType} {}

    Function(string name, DataType returnType, vector<DataType> parameterTypes)
        : name{move(name)}, returnType{returnType}, parameterTypes{move(parameterTypes)} {}

public:
    string name;
    DataType returnType;
    vector<DataType> parameterTypes;
};

class CountStarFunc : public Function {

public:
    CountStarFunc() : Function{COUNT_STAR_FUNC_NAME, INT64} {}
};

class IDFunc : public Function {

public:
    IDFunc() : Function{ID_FUNC_NAME, NODE_ID, vector<DataType>{NODE}} {}
};

class DateFunc : public Function {

public:
    DateFunc() : Function{DATE_FUNC_NAME, DATE, vector<DataType>{STRING}} {}
};

} // namespace common
} // namespace graphflow
