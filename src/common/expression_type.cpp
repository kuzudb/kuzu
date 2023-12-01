#include "common/enums/expression_type.h"

#include "common/assert.h"

namespace kuzu {
namespace common {

bool isExpressionUnary(ExpressionType type) {
    return ExpressionType::NOT == type || ExpressionType::IS_NULL == type ||
           ExpressionType::IS_NOT_NULL == type;
}

bool isExpressionBinary(ExpressionType type) {
    return isExpressionComparison(type) || ExpressionType::OR == type ||
           ExpressionType::XOR == type || ExpressionType::AND == type;
}

bool isExpressionBoolConnection(ExpressionType type) {
    return ExpressionType::OR == type || ExpressionType::XOR == type ||
           ExpressionType::AND == type || ExpressionType::NOT == type;
}

bool isExpressionComparison(ExpressionType type) {
    return ExpressionType::EQUALS == type || ExpressionType::NOT_EQUALS == type ||
           ExpressionType::GREATER_THAN == type || ExpressionType::GREATER_THAN_EQUALS == type ||
           ExpressionType::LESS_THAN == type || ExpressionType::LESS_THAN_EQUALS == type;
}

bool isExpressionNullOperator(ExpressionType type) {
    return ExpressionType::IS_NULL == type || ExpressionType::IS_NOT_NULL == type;
}

bool isExpressionLiteral(ExpressionType type) {
    return ExpressionType::LITERAL == type;
}

bool isExpressionAggregate(ExpressionType type) {
    return ExpressionType::AGGREGATE_FUNCTION == type;
}

bool isExpressionSubquery(ExpressionType type) {
    return ExpressionType::SUBQUERY == type;
}

// LCOV_EXCL_START
std::string expressionTypeToString(ExpressionType type) {
    switch (type) {
    case ExpressionType::OR:
        return "OR";
    case ExpressionType::XOR:
        return "XOR";
    case ExpressionType::AND:
        return "AND";
    case ExpressionType::NOT:
        return "NOT";
    case ExpressionType::EQUALS:
        return EQUALS_FUNC_NAME;
    case ExpressionType::NOT_EQUALS:
        return NOT_EQUALS_FUNC_NAME;
    case ExpressionType::GREATER_THAN:
        return GREATER_THAN_FUNC_NAME;
    case ExpressionType::GREATER_THAN_EQUALS:
        return GREATER_THAN_EQUALS_FUNC_NAME;
    case ExpressionType::LESS_THAN:
        return LESS_THAN_FUNC_NAME;
    case ExpressionType::LESS_THAN_EQUALS:
        return LESS_THAN_EQUALS_FUNC_NAME;
    case ExpressionType::IS_NULL:
        return "IS_NULL";
    case ExpressionType::IS_NOT_NULL:
        return "IS_NOT_NULL";
    case ExpressionType::PROPERTY:
        return "PROPERTY";
    case ExpressionType::LITERAL:
        return "LITERAL";
    case ExpressionType::STAR:
        return "STAR";
    case ExpressionType::VARIABLE:
        return "VARIABLE";
    case ExpressionType::PATH:
        return "PATH";
    case ExpressionType::PATTERN:
        return "PATTERN";
    case ExpressionType::PARAMETER:
        return "PARAMETER";
    case ExpressionType::FUNCTION:
        return "SCALAR_FUNCTION";
    case ExpressionType::AGGREGATE_FUNCTION:
        return "AGGREGATE_FUNCTION";
    case ExpressionType::SUBQUERY:
        return "SUBQUERY";
    default:
        KU_UNREACHABLE;
    }
    return "";
}
// LCOV_EXCL_STOP

} // namespace common
} // namespace kuzu
