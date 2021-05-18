#pragma once

#include "src/binder/include/expression/expression.h"

namespace graphflow {
namespace binder {

class LiteralExpression : public Expression {

public:
    LiteralExpression(ExpressionType expressionType, DataType dataType, const Value& literal);

    void cast(DataType dataTypeToCast) override;

public:
    Value literal;
    // A logical literal expression by default stores in a primitive ValueVector.
    // Alternatively, we store the literal in as a Vector of Value objects.
    bool storeAsPrimitiveVector;
};

} // namespace binder
} // namespace graphflow
