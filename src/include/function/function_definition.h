#pragma once

#include "binder/expression/expression.h"
#include "common/expression_type.h"

namespace kuzu {
namespace function {

struct FunctionBindData {
    common::LogicalType resultType;

    explicit FunctionBindData(common::LogicalType dataType) : resultType{std::move(dataType)} {}

    virtual ~FunctionBindData() = default;
};

struct FunctionDefinition;
using scalar_bind_func = std::function<std::unique_ptr<FunctionBindData>(
    const binder::expression_vector&, FunctionDefinition* definition)>;

struct FunctionDefinition {
    FunctionDefinition(std::string name, std::vector<common::LogicalTypeID> parameterTypeIDs,
        common::LogicalTypeID returnTypeID)
        : name{std::move(name)}, parameterTypeIDs{std::move(parameterTypeIDs)}, returnTypeID{
                                                                                    returnTypeID} {}
    FunctionDefinition(std::string name, std::vector<common::LogicalTypeID> parameterTypeIDs,
        common::LogicalTypeID returnTypeID, scalar_bind_func bindFunc)
        : name{std::move(name)}, parameterTypeIDs{std::move(parameterTypeIDs)},
          returnTypeID{returnTypeID}, bindFunc{std::move(bindFunc)} {}

    inline std::string signatureToString() const {
        std::string result = common::LogicalTypeUtils::dataTypesToString(parameterTypeIDs);
        result += " -> " + common::LogicalTypeUtils::dataTypeToString(returnTypeID);
        return result;
    }

    std::string name;
    std::vector<common::LogicalTypeID> parameterTypeIDs;
    common::LogicalTypeID returnTypeID;
    // This function is used to bind parameter/return types for functions with nested dataType.
    scalar_bind_func bindFunc;
};

} // namespace function
} // namespace kuzu
