#include "include/literal_expression.h"

#include "src/common/include/type_utils.h"

namespace graphflow {
namespace binder {

LiteralExpression::LiteralExpression(
    ExpressionType expressionType, DataType dataType, const Literal& literal)
    : Expression(expressionType, move(dataType), TypeUtils::toString(literal)), literal{literal} {
    assert(Types::isLiteralType(this->dataType.typeID));
}

LiteralExpression::LiteralExpression(
    ExpressionType expressionType, DataTypeID dataTypeID, const Literal& literal)
    : LiteralExpression{expressionType, DataType(dataTypeID), literal} {
    assert(dataTypeID != LIST);
}

} // namespace binder
} // namespace graphflow
