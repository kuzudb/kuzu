#include "src/binder/include/expression/literal_expression.h"

namespace graphflow {
namespace binder {

LiteralExpression::LiteralExpression(
    ExpressionType expressionType, DataType dataType, const Value& literal)
    : Expression(expressionType, dataType) {
    this->literal = literal;
    this->storeAsPrimitiveVector = true;
}

void LiteralExpression::cast(DataType dataTypeToCast) {
    assert(isExpressionLeafLiteral(expressionType));
    if (dataTypeToCast == UNSTRUCTURED) {
        storeAsPrimitiveVector = false;
    } else {
        assert(dataTypeToCast == STRING);
        string valAsString;
        switch (dataType) {
        case BOOL:
            valAsString = literal.primitive.booleanVal == TRUE ? "True" : "False";
            break;
        case INT32:
            valAsString = to_string(literal.primitive.int32Val);
            break;
        case DOUBLE:
            valAsString = to_string(literal.primitive.doubleVal);
            break;
        default:
            assert(false);
        }
        literal.strVal.set(valAsString);
        dataType = dataTypeToCast;
    }
}

} // namespace binder
} // namespace graphflow
