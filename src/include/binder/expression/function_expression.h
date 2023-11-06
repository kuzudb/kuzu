#pragma once

#include "expression.h"
#include "function/aggregate_function.h"
#include "function/scalar_function.h"

namespace kuzu {
namespace binder {

class FunctionExpression : public Expression {
public:
    FunctionExpression(std::string functionName, common::ExpressionType expressionType,
        std::unique_ptr<function::FunctionBindData> bindData, const std::string& uniqueName)
        : Expression{expressionType, bindData->resultType, uniqueName},
          functionName{std::move(functionName)}, bindData{std::move(bindData)} {}

    FunctionExpression(std::string functionName, common::ExpressionType expressionType,
        std::unique_ptr<function::FunctionBindData> bindData,
        const std::shared_ptr<Expression>& child, const std::string& uniqueName)
        : Expression{expressionType, bindData->resultType, child, uniqueName},
          functionName{std::move(functionName)}, bindData{std::move(bindData)} {}

    FunctionExpression(std::string functionName, common::ExpressionType expressionType,
        std::unique_ptr<function::FunctionBindData> bindData, expression_vector children,
        const std::string& uniqueName)
        : Expression{expressionType, bindData->resultType, std::move(children), uniqueName},
          functionName{std::move(functionName)}, bindData{std::move(bindData)} {}

    virtual ~FunctionExpression() = default;

    inline std::string getFunctionName() const { return functionName; }
    inline function::FunctionBindData* getBindData() const { return bindData.get(); }

    std::string toStringInternal() const override = 0;

protected:
    std::string functionName;
    std::unique_ptr<function::FunctionBindData> bindData;
};

class ScalarFunctionExpression : public FunctionExpression {
public:
    ScalarFunctionExpression(std::string functionName, common::ExpressionType expressionType,
        std::unique_ptr<function::FunctionBindData> bindData, expression_vector children,
        function::scalar_exec_func execFunc, function::scalar_select_func selectFunc,
        const std::string& uniqueName)
        : ScalarFunctionExpression{std::move(functionName), expressionType, std::move(bindData),
              std::move(children), std::move(execFunc), std::move(selectFunc), nullptr,
              uniqueName} {}

    ScalarFunctionExpression(std::string functionName, common::ExpressionType expressionType,
        std::unique_ptr<function::FunctionBindData> bindData, expression_vector children,
        function::scalar_exec_func execFunc, function::scalar_select_func selectFunc,
        function::scalar_compile_func compileFunc, const std::string& uniqueName)
        : FunctionExpression{std::move(functionName), expressionType, std::move(bindData),
              std::move(children), uniqueName},
          execFunc{std::move(execFunc)}, selectFunc{std::move(selectFunc)}, compileFunc{std::move(
                                                                                compileFunc)} {}

    static std::string getUniqueName(const std::string& functionName, expression_vector& children);

    std::string toStringInternal() const final;

public:
    function::scalar_exec_func execFunc;
    function::scalar_select_func selectFunc;
    function::scalar_compile_func compileFunc;
};

class AggregateFunctionExpression : public FunctionExpression {
public:
    AggregateFunctionExpression(std::string functionName,
        std::unique_ptr<function::FunctionBindData> bindData,
        std::unique_ptr<function::AggregateFunction> aggregateFunction,
        const std::string& uniqueName)
        : AggregateFunctionExpression{std::move(functionName), std::move(bindData),
              expression_vector{}, std::move(aggregateFunction), uniqueName} {}

    AggregateFunctionExpression(std::string functionName,
        std::unique_ptr<function::FunctionBindData> bindData, expression_vector children,
        std::unique_ptr<function::AggregateFunction> aggregateFunction,
        const std::string& uniqueName)
        : FunctionExpression{std::move(functionName), common::AGGREGATE_FUNCTION,
              std::move(bindData), std::move(children), uniqueName},
          aggregateFunction{std::move(aggregateFunction)} {}

    static std::string getUniqueName(
        const std::string& functionName, expression_vector& children, bool isDistinct);

    inline bool isDistinct() const { return aggregateFunction->isFunctionDistinct(); }

    std::string toStringInternal() const final;

public:
    std::unique_ptr<function::AggregateFunction> aggregateFunction;
};

} // namespace binder
} // namespace kuzu
