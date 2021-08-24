#pragma once

#include "src/binder/include/expression/expression.h"
#include "src/common/include/literal.h"

namespace graphflow {
namespace binder {

class LiteralExpression : public Expression {

public:
    LiteralExpression(ExpressionType expressionType, DataType dataType, const Literal& literal);

    void castToString();

    unique_ptr<Expression> copy() override {
        return make_unique<LiteralExpression>(expressionType, dataType, Literal(literal));
    }

public:
    Literal literal;
};

} // namespace binder
} // namespace graphflow
