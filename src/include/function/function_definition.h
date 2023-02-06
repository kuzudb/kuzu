#pragma once

#include "common/expression_type.h"

namespace kuzu {
namespace function {

struct FunctionDefinition {

    FunctionDefinition(std::string name, std::vector<common::DataTypeID> parameterTypeIDs,
        common::DataTypeID returnTypeID)
        : name{std::move(name)}, parameterTypeIDs{std::move(parameterTypeIDs)}, returnTypeID{
                                                                                    returnTypeID} {}

    inline std::string signatureToString() const {
        std::string result = common::Types::dataTypesToString(parameterTypeIDs);
        result += " -> " + common::Types::dataTypeToString(returnTypeID);
        return result;
    }

    std::string name;
    std::vector<common::DataTypeID> parameterTypeIDs;
    common::DataTypeID returnTypeID;
};

} // namespace function
} // namespace kuzu
