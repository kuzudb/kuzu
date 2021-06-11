#include "src/binder/include/expression/literal_expression.h"

namespace graphflow {
namespace binder {

LiteralExpression::LiteralExpression(
    ExpressionType expressionType, DataType dataType, const Literal& literal)
    : Expression(expressionType, dataType), literal{literal} {
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
        dataType = dataTypeToCast;
    }
}

} // namespace binder
} // namespace graphflow
