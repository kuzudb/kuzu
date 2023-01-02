#pragma once

#include "common/expression_type.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

struct FunctionDefinition {

    FunctionDefinition(string name, vector<DataTypeID> parameterTypeIDs, DataTypeID returnTypeID)
        : name{move(name)}, parameterTypeIDs{move(parameterTypeIDs)}, returnTypeID{returnTypeID} {}

    inline string signatureToString() const {
        string result = Types::dataTypesToString(parameterTypeIDs);
        result += " -> " + Types::dataTypeToString(returnTypeID);
        return result;
    }

    string name;
    vector<DataTypeID> parameterTypeIDs;
    DataTypeID returnTypeID;
};

} // namespace function
} // namespace kuzu
