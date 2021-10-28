#include "src/binder/include/expression/literal_expression.h"

#include "src/common/include/date.h"

namespace graphflow {
namespace binder {

LiteralExpression::LiteralExpression(
    ExpressionType expressionType, DataType dataType, const Literal& literal)
    : Expression(expressionType, dataType), literal{literal} {
    assert(dataType == BOOL || dataType == INT64 || dataType == DOUBLE || dataType == STRING ||
           dataType == DATE || dataType == TIMESTAMP || dataType == INTERVAL);
}

void LiteralExpression::castToString() {
    literal.castToString();
    dataType = STRING;
}

} // namespace binder
} // namespace graphflow
