#pragma once

#include "expression.h"
#include "function/aggregate/aggregate_function.h"
#include "function/vector_operations.h"

namespace kuzu {
namespace binder {

class FunctionExpression : public Expression {

protected:
    FunctionExpression(common::ExpressionType expressionType, common::DataType dataType,
        const std::string& uniqueName)
        : Expression{expressionType, std::move(dataType), uniqueName} {}

    FunctionExpression(common::ExpressionType expressionType, common::DataType dataType,
        const std::shared_ptr<Expression>& child, const std::string& uniqueName)
        : Expression{expressionType, std::move(dataType), child, uniqueName} {}

    FunctionExpression(common::ExpressionType expressionType, common::DataType dataType,
        expression_vector children, const std::string& uniqueName)
        : Expression{expressionType, std::move(dataType), std::move(children), uniqueName} {}
};

class ScalarFunctionExpression : public FunctionExpression {

public:
    ScalarFunctionExpression(common::ExpressionType expressionType,
        const common::DataType& dataType, expression_vector children,
        function::scalar_exec_func execFunc, function::scalar_select_func selectFunc,
        const std::string& uniqueName)
        : FunctionExpression{expressionType, dataType, std::move(children), uniqueName},
          execFunc{std::move(execFunc)}, selectFunc{std::move(selectFunc)} {}

    static inline std::string getUniqueName(
        const std::string& functionName, expression_vector& children) {
        auto result = functionName + "(";
        for (auto& child : children) {
            result += child->getUniqueName() + ", ";
        }
        result += ")";
        return result;
    }

public:
    function::scalar_exec_func execFunc;
    function::scalar_select_func selectFunc;
};

class AggregateFunctionExpression : public FunctionExpression {

public:
    AggregateFunctionExpression(const common::DataType& dataType,
        std::unique_ptr<function::AggregateFunction> aggregateFunction,
        const std::string& uniqueName)
        : AggregateFunctionExpression{
              dataType, expression_vector{}, std::move(aggregateFunction), uniqueName} {}

    AggregateFunctionExpression(const common::DataType& dataType, expression_vector children,
        std::unique_ptr<function::AggregateFunction> aggregateFunction,
        const std::string& uniqueName)
        : FunctionExpression{common::AGGREGATE_FUNCTION, dataType, std::move(children), uniqueName},
          aggregateFunction{std::move(aggregateFunction)} {}

    static inline std::string getUniqueName(
        const std::string& functionName, expression_vector& children, bool isDistinct) {
        auto result = functionName + "(";
        if (isDistinct) {
            result += "DISTINCT ";
        }
        for (auto& child : children) {
            result += child->getUniqueName() + ", ";
        }
        result += ")";
        return result;
    }

    inline bool isDistinct() const { return aggregateFunction->isFunctionDistinct(); }

public:
    std::unique_ptr<function::AggregateFunction> aggregateFunction;
};

} // namespace binder
} // namespace kuzu
