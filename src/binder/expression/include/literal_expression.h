#pragma once

#include "expression.h"

#include "src/common/types/include/literal.h"

namespace graphflow {
namespace binder {

class LiteralExpression : public Expression {

public:
    LiteralExpression(ExpressionType expressionType, DataType dataType, const Literal& literal);
    LiteralExpression(ExpressionType expressionType, DataTypeID dataTypeID, const Literal& literal);

public:
    Literal literal;
};

} // namespace binder
} // namespace graphflow
