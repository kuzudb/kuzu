#include "src/common/include/expression_type.h"

namespace graphflow {
namespace common {

bool isExpressionUnary(EXPRESSION_TYPE type) {
    return NOT == type || NEGATE == type;
}

bool isExpressionBinary(EXPRESSION_TYPE type) {
    return !isExpressionUnary(type);
}

bool isExpressionLeafLiteral(EXPRESSION_TYPE type) {
    return LITERAL_INT == type || LITERAL_DOUBLE == type || LITERAL_STRING == type ||
           LITERAL_BOOLEAN == type || LITERAL_NULL == type;
}

bool isExpressionLeafVariable(EXPRESSION_TYPE type) {
    return VARIABLE == type;
}

DataType getUnaryExpressionResultDataType(EXPRESSION_TYPE type, DataType operandType) {
    switch (type) {
    case NOT:
        return BOOL;
    case NEGATE:
        return operandType;
    default:
        throw std::invalid_argument("Invalid or unsupported unary operation.");
    }
}

DataType getBinaryExpressionResultDataType(
    EXPRESSION_TYPE type, DataType leftOperandType, DataType rightOperandType) {
    switch (type) {
    case AND:
    case OR:
    case XOR:
    case EQUALS:
    case NOT_EQUALS:
    case GREATER_THAN:
    case GREATER_THAN_EQUALS:
    case LESS_THAN:
    case LESS_THAN_EQUALS:
        return BOOL;
    case ADD:
    case SUBTRACT:
    case MULTIPLY:
    case DIVIDE:
    case MODULO:
    case NEGATE:
        if (leftOperandType == DOUBLE || rightOperandType == DOUBLE) {
            return DOUBLE;
        }
        return INT;
    case POWER:
        return DOUBLE;
    default:
        throw std::invalid_argument("Invalid or unsupported binary operation.");
    }
}

} // namespace common
} // namespace graphflow
