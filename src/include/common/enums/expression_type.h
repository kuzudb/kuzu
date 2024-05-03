#pragma once

#include <cstdint>
#include <string>

namespace kuzu {
namespace common {

enum class ExpressionType : uint8_t {

    // Boolean Connection Expressions
    OR = 0,
    XOR = 1,
    AND = 2,
    NOT = 3,

    // Comparison Expressions
    EQUALS = 10,
    NOT_EQUALS = 11,
    GREATER_THAN = 12,
    GREATER_THAN_EQUALS = 13,
    LESS_THAN = 14,
    LESS_THAN_EQUALS = 15,

    // Null Operator Expressions
    IS_NULL = 50,
    IS_NOT_NULL = 51,

    PROPERTY = 60,

    LITERAL = 70,

    STAR = 80,

    VARIABLE = 90,
    PATH = 91,
    PATTERN = 92, // Node & Rel pattern

    PARAMETER = 100,

    FUNCTION = 110,

    AGGREGATE_FUNCTION = 130,

    SUBQUERY = 190,

    CASE_ELSE = 200,
};

bool isExpressionUnary(ExpressionType type);
bool isExpressionBinary(ExpressionType type);
bool isExpressionBoolConnection(ExpressionType type);
bool isExpressionComparison(ExpressionType type);
bool isExpressionNullOperator(ExpressionType type);
bool isExpressionLiteral(ExpressionType type);
bool isExpressionAggregate(ExpressionType type);
bool isExpressionSubquery(ExpressionType type);

std::string expressionTypeToString(ExpressionType type);

} // namespace common
} // namespace kuzu
