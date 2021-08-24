#include "src/binder/include/expression/literal_expression.h"

#include "src/common/include/date.h"

namespace graphflow {
namespace binder {

LiteralExpression::LiteralExpression(
    ExpressionType expressionType, DataType dataType, const Literal& literal)
    : Expression(expressionType, dataType), literal{literal} {
    assert(dataType == BOOL || dataType == INT64 || dataType == DOUBLE || dataType == STRING ||
           dataType == DATE);
}

void LiteralExpression::castToString() {
    string valAsString;
    switch (dataType) {
    case BOOL:
        valAsString = TypeUtils::toString(literal.val.booleanVal);
        break;
    case INT64:
        valAsString = TypeUtils::toString(literal.val.int64Val);
        break;
    case DOUBLE:
        valAsString = TypeUtils::toString(literal.val.doubleVal);
        break;
    case DATE:
        valAsString = Date::toString(literal.val.dateVal);
        break;
    default:
        assert(false);
    }
    literal.strVal = valAsString;
    literal.dataType = STRING;
    dataType = STRING;
}

} // namespace binder
} // namespace graphflow
