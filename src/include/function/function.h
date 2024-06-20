#pragma once

#include "binder/expression/expression.h"
#include "common/api.h"

namespace kuzu {

namespace main {
class ClientContext;
}

namespace function {

struct KUZU_API FunctionBindData {
    std::vector<common::LogicalType> paramTypes;
    common::LogicalType resultType;
    main::ClientContext* clientContext;
    int64_t count;

    explicit FunctionBindData(common::LogicalType dataType)
        : resultType{std::move(dataType)}, clientContext{nullptr}, count{1} {}
    FunctionBindData(std::vector<common::LogicalType> paramTypes, common::LogicalType resultType)
        : paramTypes{std::move(paramTypes)}, resultType{std::move(resultType)},
          clientContext{nullptr}, count{1} {}
    DELETE_COPY_AND_MOVE(FunctionBindData);
    virtual ~FunctionBindData() = default;

    static std::unique_ptr<FunctionBindData> getSimpleBindData(
        const binder::expression_vector& params, const common::LogicalType& resultType);

    template<class TARGET>
    TARGET& cast() {
        return common::ku_dynamic_cast<FunctionBindData&, TARGET&>(*this);
    }

    virtual std::unique_ptr<FunctionBindData> copy() const {
        return std::make_unique<FunctionBindData>(common::LogicalType::copy(paramTypes),
            resultType.copy());
    }
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
