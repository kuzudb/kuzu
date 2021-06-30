#include "src/binder/include/expression/literal_expression.h"

namespace graphflow {
namespace binder {
LiteralExpression::LiteralExpression(
    ExpressionType expressionType, DataType dataType, const Literal& literal)
    : Expression(expressionType, dataType), literal{literal} {}

void LiteralExpression::castToString() {
    string valAsString;
    switch (dataType) {
    case BOOL:
        valAsString = literal.val.booleanVal == TRUE ? "True" : "False";
        break;
    case INT32:
        valAsString = to_string(literal.val.int32Val);
        break;
    case DOUBLE:
        valAsString = to_string(literal.val.doubleVal);
        break;
    default:
        assert(false);
    }
    literal.strVal = valAsString;
    dataType = STRING;
}

} // namespace binder
} // namespace graphflow
