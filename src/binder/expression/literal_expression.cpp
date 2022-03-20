#include "include/literal_expression.h"

#include "src/common/include/type_utils.h"

namespace graphflow {
namespace binder {

LiteralExpression::LiteralExpression(
    ExpressionType expressionType, DataType dataType, const Literal& literal)
    : Expression(expressionType, dataType, TypeUtils::toString(literal)), literal{literal} {
    assert(dataType == BOOL || dataType == INT64 || dataType == DOUBLE || dataType == STRING ||
           dataType == DATE || dataType == TIMESTAMP || dataType == INTERVAL);
}

void LiteralExpression::castToString() {
    TypeUtils::castLiteralToString(literal);
    dataType = STRING;
}

} // namespace binder
} // namespace graphflow
