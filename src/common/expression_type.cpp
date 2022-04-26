#include "src/common/include/expression_type.h"

#include "src/common/include/exception.h"

namespace graphflow {
namespace common {

bool isExpressionUnary(ExpressionType type) {
    return NOT == type || IS_NULL == type || IS_NOT_NULL == type;
}

bool isExpressionBinary(ExpressionType type) {
    return isExpressionComparison(type) || OR == type || XOR == type || AND == type;
}

bool isExpressionBoolConnection(ExpressionType type) {
    return OR == type || XOR == type || AND == type || NOT == type;
}

bool isExpressionComparison(ExpressionType type) {
    return EQUALS == type || NOT_EQUALS == type || GREATER_THAN == type ||
           GREATER_THAN_EQUALS == type || LESS_THAN == type || LESS_THAN_EQUALS == type;
}

bool isExpressionNullOperator(ExpressionType type) {
    return IS_NULL == type || IS_NOT_NULL == type;
}

bool isExpressionLiteral(ExpressionType type) {
    return LITERAL_INT == type || LITERAL_DOUBLE == type || LITERAL_STRING == type ||
           LITERAL_BOOLEAN == type || LITERAL_DATE == type || LITERAL_TIMESTAMP == type ||
           LITERAL_INTERVAL == type || LITERAL_NULL == type;
}

bool isExpressionAggregate(ExpressionType type) {
    return AGGREGATE_FUNCTION == type;
}

bool isExpressionSubquery(ExpressionType type) {
    return EXISTENTIAL_SUBQUERY == type;
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
        return EQUALS_FUNC_NAME;
    case NOT_EQUALS:
        return NOT_EQUALS_FUNC_NAME;
    case GREATER_THAN:
        return GREATER_THAN_FUNC_NAME;
    case GREATER_THAN_EQUALS:
        return GREATER_THAN_EQUALS_FUNC_NAME;
    case LESS_THAN:
        return LESS_THAN_FUNC_NAME;
    case LESS_THAN_EQUALS:
        return LESS_THAN_EQUALS_FUNC_NAME;
    case IS_NULL:
        return "IS_NULL";
    case IS_NOT_NULL:
        return "IS_NOT_NULL";
    case PROPERTY:
        return "PROPERTY";
    case LITERAL_INT:
        return "LITERAL_INT";
    case LITERAL_DOUBLE:
        return "LITERAL_DOUBLE";
    case LITERAL_STRING:
        return "LITERAL_STRING";
    case LITERAL_BOOLEAN:
        return "LITERAL_BOOLEAN";
    case LITERAL_DATE:
        return "LITERAL_DATE";
    case LITERAL_TIMESTAMP:
        return "LITERAL_TIMESTAMP";
    case LITERAL_NULL:
        return "LITERAL_NULL";
    case VARIABLE:
        return "VARIABLE";
    case PARAMETER:
        return "PARAMETER";
    case FUNCTION:
        return "FUNCTION";
    case AGGREGATE_FUNCTION:
        return "AGGREGATE_FUNCTION";
    case EXISTENTIAL_SUBQUERY:
        return "EXISTENTIAL_SUBQUERY";
    default:
        throw NotImplementedException("Cannot convert expression type to string");
    }
}

} // namespace common
} // namespace graphflow
