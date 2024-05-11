#pragma once

#include "binder/expression/expression.h"

namespace kuzu {

namespace main {
class ClientContext;
}

namespace function {

struct FunctionBindData {
    std::vector<common::LogicalType> paramTypes;
    std::unique_ptr<common::LogicalType> resultType;
    main::ClientContext* clientContext;

    explicit FunctionBindData(std::unique_ptr<common::LogicalType> dataType)
        : resultType{std::move(dataType)}, clientContext{nullptr} {}
    FunctionBindData(std::vector<common::LogicalType> paramTypes,
        std::unique_ptr<common::LogicalType> resultType)
        : paramTypes{std::move(paramTypes)}, resultType{std::move(resultType)},
          clientContext{nullptr} {}
    DELETE_COPY_AND_MOVE(FunctionBindData);

    virtual ~FunctionBindData() = default;

    static std::unique_ptr<FunctionBindData> getSimpleBindData(
        const binder::expression_vector& params, const common::LogicalType& resultType);
};

struct Function;
using function_set = std::vector<std::unique_ptr<Function>>;
using scalar_bind_func = std::function<std::unique_ptr<FunctionBindData>(
    const binder::expression_vector&, Function* definition)>;

struct Function {
    std::string name;
    std::vector<common::LogicalTypeID> parameterTypeIDs;
    // Currently we only one variable-length function which is list creation. The expectation is
    // that all parameters must have the same type as parameterTypes[0].
    bool isVarLength;

    Function() : isVarLength{false} {};
    Function(std::string name, std::vector<common::LogicalTypeID> parameterTypeIDs)
        : name{std::move(name)}, parameterTypeIDs{std::move(parameterTypeIDs)}, isVarLength{false} {
    }
    Function(const Function&) = default;

    virtual ~Function() = default;

    virtual std::string signatureToString() const {
        return common::LogicalTypeUtils::toString(parameterTypeIDs);
    }

    virtual std::unique_ptr<Function> copy() const = 0;

    template<class TARGET>
    const TARGET* constPtrCast() const {
        return common::ku_dynamic_cast<const Function*, const TARGET*>(this);
    }
    template<class TARGET>
    TARGET* ptrCast() {
        return common::ku_dynamic_cast<Function*, TARGET*>(this);
    }
};

struct BaseScalarFunction : public Function {
    common::LogicalTypeID returnTypeID;
    scalar_bind_func bindFunc;

    BaseScalarFunction(std::string name, std::vector<common::LogicalTypeID> parameterTypeIDs,
        common::LogicalTypeID returnTypeID, scalar_bind_func bindFunc)
        : Function{std::move(name), std::move(parameterTypeIDs)}, returnTypeID{returnTypeID},
          bindFunc{std::move(bindFunc)} {}

    std::string signatureToString() const override {
        auto result = Function::signatureToString();
        result += " -> " + common::LogicalTypeUtils::toString(returnTypeID);
        return result;
    }
};

} // namespace function
} // namespace kuzu
