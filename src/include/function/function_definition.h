#pragma once

#include "common/expression_type.h"

namespace kuzu {
namespace function {

// Forward declaration of FunctionDefinition.
struct FunctionDefinition;

using scalar_bind_func = std::function<void(
    const std::vector<common::DataType>&, FunctionDefinition*, common::DataType&)>;

struct FunctionDefinition {

    FunctionDefinition(std::string name, std::vector<common::DataTypeID> parameterTypeIDs,
        common::DataTypeID returnTypeID)
        : name{std::move(name)}, parameterTypeIDs{std::move(parameterTypeIDs)}, returnTypeID{
                                                                                    returnTypeID} {}
    FunctionDefinition(std::string name, std::vector<common::DataTypeID> parameterTypeIDs,
        common::DataTypeID returnTypeID, scalar_bind_func bindFunc)
        : name{std::move(name)}, parameterTypeIDs{std::move(parameterTypeIDs)},
          returnTypeID{returnTypeID}, bindFunc{std::move(bindFunc)} {}

    inline std::string signatureToString() const {
        std::string result = common::Types::dataTypesToString(parameterTypeIDs);
        result += " -> " + common::Types::dataTypeToString(returnTypeID);
        return result;
    }

    std::string name;
    std::vector<common::DataTypeID> parameterTypeIDs;
    common::DataTypeID returnTypeID;
    // This function is used to bind parameter/return types for functions with nested dataType.
    scalar_bind_func bindFunc;
};

} // namespace function
} // namespace kuzu
