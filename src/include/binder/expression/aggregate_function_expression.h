#pragma once

#include "expression.h"
#include "function/aggregate_function.h"

namespace kuzu {
namespace binder {

class AggregateFunctionExpression : public Expression {
    static constexpr common::ExpressionType expressionType_ =
        common::ExpressionType::AGGREGATE_FUNCTION;

public:
    AggregateFunctionExpression(function::AggregateFunction function,
        std::unique_ptr<function::FunctionBindData> bindData, expression_vector children,
        std::string uniqueName)
        : Expression{expressionType_, bindData->resultType.copy(), std::move(children), uniqueName},
          function{std::move(function)}, bindData{std::move(bindData)} {}

    const function::AggregateFunction& getFunction() const { return function; }
    function::FunctionBindData* getBindData() const { return bindData.get(); }
    bool isDistinct() const { return function.isDistinct; }

    std::string toStringInternal() const final;

    static std::string getUniqueName(const std::string& functionName,
        const expression_vector& children, bool isDistinct);

private:
    function::AggregateFunction function;
    std::unique_ptr<function::FunctionBindData> bindData;
};

} // namespace binder
} // namespace kuzu
