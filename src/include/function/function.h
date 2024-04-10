#pragma once

#include "binder/expression/expression.h"

namespace kuzu {
namespace function {

struct FunctionBindData {
    std::unique_ptr<common::LogicalType> resultType;

    explicit FunctionBindData(std::unique_ptr<common::LogicalType> dataType)
        : resultType{std::move(dataType)} {}

    virtual ~FunctionBindData() = default;
};

struct Function;
using function_set = std::vector<std::unique_ptr<Function>>;
using scalar_bind_func = std::function<std::unique_ptr<FunctionBindData>(
    const binder::expression_vector&, Function* definition)>;

enum class FunctionType : uint8_t {
    UNKNOWN = 0,
    SCALAR = 1,
    REWRITE = 2,
    AGGREGATE = 3,
    TABLE = 4
};

struct Function {
    Function() : type{FunctionType::UNKNOWN} {};
    Function(FunctionType type, std::string name,
        std::vector<common::LogicalTypeID> parameterTypeIDs)
        : type{type}, name{std::move(name)}, parameterTypeIDs{std::move(parameterTypeIDs)} {}

    virtual ~Function() = default;

    virtual std::string signatureToString() const {
        return common::LogicalTypeUtils::toString(parameterTypeIDs);
    }

    virtual std::unique_ptr<Function> copy() const = 0;

    // TODO(Ziyi): Move to catalog entry once we have implemented the catalog entry.
    FunctionType type;
    std::string name;
    std::vector<common::LogicalTypeID> parameterTypeIDs;
};

struct BaseScalarFunction : public Function {
    BaseScalarFunction(FunctionType type, std::string name,
        std::vector<common::LogicalTypeID> parameterTypeIDs, common::LogicalTypeID returnTypeID,
        scalar_bind_func bindFunc)
        : Function{type, std::move(name), std::move(parameterTypeIDs)}, returnTypeID{returnTypeID},
          bindFunc{std::move(bindFunc)} {}

    std::string signatureToString() const override {
        auto result = Function::signatureToString();
        result += " -> " + common::LogicalTypeUtils::toString(returnTypeID);
        return result;
    }

    common::LogicalTypeID returnTypeID;
    scalar_bind_func bindFunc;
};

} // namespace function
} // namespace kuzu
