#pragma once

#include "binder/expression/expression.h"

namespace kuzu {
namespace function {

struct FunctionBindData {
    std::vector<common::LogicalType> paramTypes;
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
    Function() : type{FunctionType::UNKNOWN}, varLength{false} {};
    Function(FunctionType type, std::string name,
        std::vector<common::LogicalTypeID> parameterTypeIDs)
        : type{type}, name{std::move(name)}, parameterTypeIDs{std::move(parameterTypeIDs)},
          varLength{false} {}
    Function(const Function& other)
        : type{other.type}, name{other.name}, parameterTypeIDs{other.parameterTypeIDs},
          varLength{other.varLength} {}

    virtual ~Function() = default;

    virtual std::string signatureToString() const {
        return common::LogicalTypeUtils::toString(parameterTypeIDs);
    }

    virtual std::unique_ptr<Function> copy() const = 0;

    // TODO(Ziyi): Move to catalog entry once we have implemented the catalog entry.
    FunctionType type;
    std::string name;
    // We do NOT store logical type because we cannot register a nested type parameter without
    // seeing children expressions.
    std::vector<common::LogicalTypeID> parameterTypeIDs;
    // Whether function can have arbitrary number of parameters.
    bool varLength;

    template<class TARGET>
    const TARGET* constPtrCast() const {
        return common::ku_dynamic_cast<const Function*, const TARGET*>(this);
    }
};

struct BaseScalarFunction : public Function {
    BaseScalarFunction(FunctionType type, std::string name,
        std::vector<common::LogicalTypeID> parameterTypeIDs, common::LogicalTypeID returnTypeID,
        scalar_bind_func bindFunc)
        : Function{type, std::move(name), std::move(parameterTypeIDs)}, returnTypeID{returnTypeID},
          bindFunc{std::move(bindFunc)} {}
    BaseScalarFunction(const BaseScalarFunction& other)
        : Function{other}, returnTypeID{other.returnTypeID}, bindFunc{other.bindFunc} {}

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
