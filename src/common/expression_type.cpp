#include "src/common/include/expression_type.h"

#include <stdexcept>

namespace graphflow {
namespace common {

bool isExpressionUnary(ExpressionType type) {
    return NOT == type || NEGATE == type || IS_NULL == type || IS_NOT_NULL == type ||
           CAST_TO_STRING == type || CAST_TO_UNSTRUCTURED_VECTOR == type ||
           CAST_UNSTRUCTURED_VECTOR_TO_BOOL_VECTOR == type;
}

bool isExpressionBinary(ExpressionType type) {
    return !isExpressionUnary(type);
}

bool isExpressionBoolConnection(ExpressionType type) {
    return OR == type || XOR == type || AND == type || NOT == type;
}

bool isExpressionComparison(ExpressionType type) {
    return EQUALS == type || NOT_EQUALS == type || GREATER_THAN == type ||
           GREATER_THAN_EQUALS == type || LESS_THAN == type || LESS_THAN_EQUALS == type;
}

bool isExpressionArithmetic(ExpressionType type) {
    return ADD == type || SUBTRACT == type || MULTIPLY == type || DIVIDE == type ||
           MODULO == type || POWER == type || NEGATE == type;
}

bool isExpressionStringOperator(ExpressionType type) {
    return STARTS_WITH == type || ENDS_WITH == type || CONTAINS == type;
}

bool isExpressionNullComparison(ExpressionType type) {
    return IS_NULL == type || IS_NOT_NULL == type;
}

bool isExpressionLiteral(ExpressionType type) {
    return LITERAL_INT == type || LITERAL_DOUBLE == type || LITERAL_STRING == type ||
           LITERAL_BOOLEAN == type || LITERAL_DATE == type || LITERAL_NULL == type;
}

ExpressionType comparisonToIDComparison(ExpressionType type) {
    switch (type) {
    case EQUALS:
        return EQUALS_NODE_ID;
    case NOT_EQUALS:
        return NOT_EQUALS_NODE_ID;
    case GREATER_THAN:
        return GREATER_THAN_NODE_ID;
    case GREATER_THAN_EQUALS:
        return GREATER_THAN_EQUALS_NODE_ID;
    case LESS_THAN:
        return LESS_THAN_NODE_ID;
    case LESS_THAN_EQUALS:
        return LESS_THAN_EQUALS_NODE_ID;
    default:
        throw invalid_argument("Cannot map " + expressionTypeToString(type) + " to ID comparison.");
    }
}

string expressionTypeToString(ExpressionType type) {
    switch (type) {
    case OR:
        return "OR";
    case XOR:
        return "XOR";
    case AND:
        return "AND";
    case NOT:
        return "NOT";
    case EQUALS:
        return "EQUALS";
    case NOT_EQUALS:
        return "NOT_EQUALS";
    case GREATER_THAN:
        return "GREATER_THAN";
    case GREATER_THAN_EQUALS:
        return "GREATER_THAN_EQUALS";
    case LESS_THAN:
        return "LESS_THAN";
    case LESS_THAN_EQUALS:
        return "LESS_THAN_EQUALS";
    case ADD:
        return "ADD";
    case SUBTRACT:
        return "SUBTRACT";
    case MULTIPLY:
        return "MULTIPLY";
    case DIVIDE:
        return "DIVIDE";
    case MODULO:
        return "MODULO";
    case POWER:
        return "POWER";
    case NEGATE:
        return "NEGATE";
    case HASH_NODE_ID:
        return "HASH_NODE_ID";
    case STARTS_WITH:
        return "STARTS_WITH";
    case ENDS_WITH:
        return "ENDS_WITH";
    case CONTAINS:
        return "CONTAINS";
    case IS_NULL:
        return "IS_NULL";
    case IS_NOT_NULL:
        return "IS_NOT_NULL";
    case PROPERTY:
        return "PROPERTY";
    case FUNCTION:
        return "FUNCTION";
    case LITERAL_INT:
        return "LITERAL_INT";
    case LITERAL_DOUBLE:
        return "LITERAL_DOUBLE";
    case LITERAL_STRING:
        return "LITERAL_STRING";
    case LITERAL_BOOLEAN:
        return "LITERAL_BOOLEAN";
    case LITERAL_NULL:
        return "LITERAL_NULL";
    case VARIABLE:
        return "VARIABLE";
    default:
        throw invalid_argument("Should never happen. Cannot convert expression type to string");
    }
}

} // namespace common
} // namespace graphflow
