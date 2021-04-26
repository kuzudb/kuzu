#pragma once

#include "src/expression/include/logical/logical_expression.h"

namespace graphflow {
namespace expression {

class LogicalLiteralExpression : public LogicalExpression {

public:
    LogicalLiteralExpression(
        ExpressionType expressionType, DataType dataType, const Value& literal);

    void cast(DataType dataTypeToCast) override;

public:
    Value literal;
    // A logical literal expression by default stores in a primitive ValueVector.
    // Alternatively, we store the literal in as a Vector of Value objects.
    bool storeAsPrimitiveVector;
};

} // namespace expression
} // namespace graphflow
