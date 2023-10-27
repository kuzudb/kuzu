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

struct Function;
using scalar_bind_func = std::function<std::unique_ptr<FunctionBindData>(
    const binder::expression_vector&, Function* definition)>;

enum class FunctionType : uint8_t { SCALAR, AGGREGATE, TABLE };

struct Function {
    Function(
        FunctionType type, std::string name, std::vector<common::LogicalTypeID> parameterTypeIDs)
        : type{type}, name{std::move(name)}, parameterTypeIDs{std::move(parameterTypeIDs)} {}

    virtual ~Function() = default;

    // TODO(Ziyi): Move to catalog entry once we have implemented the catalog entry.
    FunctionType type;
    std::string name;
    std::vector<common::LogicalTypeID> parameterTypeIDs;
};

struct BaseScalarFunction : public Function {
    BaseScalarFunction(FunctionType type, std::string name,
        std::vector<common::LogicalTypeID> parameterTypeIDs, common::LogicalTypeID returnTypeID,
        scalar_bind_func bindFunc)
        : Function{type, std::move(name), std::move(parameterTypeIDs)},
          returnTypeID{returnTypeID}, bindFunc{std::move(bindFunc)} {}

    inline std::string signatureToString() const {
        std::string result = common::LogicalTypeUtils::dataTypesToString(parameterTypeIDs);
        result += " -> " + common::LogicalTypeUtils::dataTypeToString(returnTypeID);
        return result;
    }

    common::LogicalTypeID returnTypeID;
    // This function is used to bind parameter/return types for functions with nested dataType.
    scalar_bind_func bindFunc;
};

} // namespace function
} // namespace kuzu
