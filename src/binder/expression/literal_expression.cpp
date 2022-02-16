#include "include/literal_expression.h"

namespace graphflow {
namespace binder {

LiteralExpression::LiteralExpression(
    ExpressionType expressionType, DataType dataType, const Literal& literal)
    : Expression(expressionType, dataType, literal.toString()), literal{literal} {
    assert(dataType == BOOL || dataType == INT64 || dataType == DOUBLE || dataType == STRING ||
           dataType == DATE || dataType == TIMESTAMP || dataType == INTERVAL);
}

void LiteralExpression::castToString() {
    literal.castToString();
    dataType = STRING;
}

} // namespace binder
} // namespace graphflow
