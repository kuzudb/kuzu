#pragma once

#include "expression.h"

#include "src/common/include/literal.h"

namespace graphflow {
namespace binder {

class LiteralExpression : public Expression {

public:
    LiteralExpression(ExpressionType expressionType, DataType dataType, const Literal& literal);

    void castToString();

public:
    Literal literal;
};

} // namespace binder
} // namespace graphflow
