#pragma once

#include "expression.h"
#include "function/aggregate/aggregate_function.h"
#include "function/vector_operations.h"

namespace kuzu {
namespace binder {

class FunctionExpression : public Expression {
protected:
    FunctionExpression(std::string functionName, common::ExpressionType expressionType,
        common::DataType dataType, const std::string& uniqueName)
        : Expression{expressionType, std::move(dataType), uniqueName}, functionName{std::move(
                                                                           functionName)} {}

    FunctionExpression(std::string functionName, common::ExpressionType expressionType,
        common::DataType dataType, const std::shared_ptr<Expression>& child,
        const std::string& uniqueName)
        : Expression{expressionType, std::move(dataType), child, uniqueName},
          functionName{std::move(functionName)} {}

    FunctionExpression(std::string functionName, common::ExpressionType expressionType,
        common::DataType dataType, expression_vector children, const std::string& uniqueName)
        : Expression{expressionType, std::move(dataType), std::move(children), uniqueName},
          functionName{std::move(functionName)} {}

    virtual ~FunctionExpression() = default;

    std::string toString() const override = 0;

protected:
    std::string functionName;
};

class ScalarFunctionExpression : public FunctionExpression {
public:
    ScalarFunctionExpression(std::string functionName, common::ExpressionType expressionType,
        const common::DataType& dataType, expression_vector children,
        function::scalar_exec_func execFunc, function::scalar_select_func selectFunc,
        const std::string& uniqueName)
        : FunctionExpression{std::move(functionName), expressionType, dataType, std::move(children),
              uniqueName},
          execFunc{std::move(execFunc)}, selectFunc{std::move(selectFunc)} {}

    static std::string getUniqueName(const std::string& functionName, expression_vector& children);

    std::string toString() const override;

public:
    function::scalar_exec_func execFunc;
    function::scalar_select_func selectFunc;
};

class AggregateFunctionExpression : public FunctionExpression {
public:
    AggregateFunctionExpression(std::string functionName, const common::DataType& dataType,
        std::unique_ptr<function::AggregateFunction> aggregateFunction,
        const std::string& uniqueName)
        : AggregateFunctionExpression{std::move(functionName), dataType, expression_vector{},
              std::move(aggregateFunction), uniqueName} {}

    AggregateFunctionExpression(std::string functionName, const common::DataType& dataType,
        expression_vector children, std::unique_ptr<function::AggregateFunction> aggregateFunction,
        const std::string& uniqueName)
        : FunctionExpression{std::move(functionName), common::AGGREGATE_FUNCTION, dataType,
              std::move(children), uniqueName},
          aggregateFunction{std::move(aggregateFunction)} {}

    static std::string getUniqueName(
        const std::string& functionName, expression_vector& children, bool isDistinct);

    inline bool isDistinct() const { return aggregateFunction->isFunctionDistinct(); }

    std::string toString() const override;

public:
    std::unique_ptr<function::AggregateFunction> aggregateFunction;
};

} // namespace binder
} // namespace kuzu
