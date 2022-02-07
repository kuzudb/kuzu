#pragma once

#include "src/binder/include/expression/expression.h"

namespace graphflow {
namespace binder {

class FunctionExpression : public Expression {

public:
    FunctionExpression(ExpressionType expressionType, DataType dataType, const string& uniqueName,
        bool isDistinct = false)
        : Expression{expressionType, dataType, uniqueName}, isDistinct{isDistinct} {}

    FunctionExpression(ExpressionType expressionType, DataType dataType,
        const shared_ptr<Expression>& child, bool isDistinct = false)
        : Expression{expressionType, dataType, child}, isDistinct{isDistinct} {}

    bool isFunctionDistinct() const {
        assert(isExpressionAggregate(expressionType));
        return isDistinct;
    }

private:
    bool isDistinct;
};

} // namespace binder
} // namespace graphflow
