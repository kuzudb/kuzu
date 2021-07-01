#pragma once

#include <cstdint>
#include <string>

using namespace std;

namespace graphflow {
namespace common {

enum ExpressionType : uint8_t {

    /**
     * Boolean Connection Expressions
     **/
    OR = 0,
    XOR = 1,
    AND = 2,
    NOT = 3,

    /**
     * Comparison Expressions
     **/
    EQUALS = 10,
    NOT_EQUALS = 11,
    GREATER_THAN = 12,
    GREATER_THAN_EQUALS = 13,
    LESS_THAN = 14,
    LESS_THAN_EQUALS = 15,

    /**
     * Arithmetic Expressions
     **/
    ADD = 20,
    SUBTRACT = 21,
    MULTIPLY = 22,
    DIVIDE = 23,
    MODULO = 24,
    POWER = 25,
    NEGATE = 26,

    /**
     * NODE ID Expressions
     * */
    HASH_NODE_ID = 32,
    EQUALS_NODE_ID = 33,
    NOT_EQUALS_NODE_ID = 34,
    GREATER_THAN_NODE_ID = 35,
    GREATER_THAN_EQUALS_NODE_ID = 36,
    LESS_THAN_NODE_ID = 37,
    LESS_THAN_EQUALS_NODE_ID = 38,

    /**
     * String Operator Expressions
     **/
    STARTS_WITH = 40,
    ENDS_WITH = 41,
    CONTAINS = 42,

    /**
     * List Operator Expressions works only for CSV Line
     */
    CSV_LINE_EXTRACT = 45,

    /**
     * Null Operator Expressions
     **/
    IS_NULL = 50,
    IS_NOT_NULL = 51,

    /**
     * Property Expression
     **/
    PROPERTY = 60,

    /**
     * Function Expression
     **/
    FUNCTION = 70,

    /**
     * Literal Expressions
     **/
    LITERAL_INT = 80,
    LITERAL_DOUBLE = 81,
    LITERAL_STRING = 82,
    LITERAL_BOOLEAN = 83,
    LITERAL_NULL = 84,

    /**
     * Variable Expression
     **/
    VARIABLE = 90,

    /**
     * Cast Expressions
     ***/
    CAST_TO_STRING = 100,
    CAST_TO_UNSTRUCTURED_VECTOR = 101,
    CAST_UNSTRUCTURED_VECTOR_TO_BOOL_VECTOR = 102,
};

bool isExpressionUnary(ExpressionType type);
bool isExpressionBinary(ExpressionType type);
bool isExpressionBoolConnection(ExpressionType type);
bool isExpressionComparison(ExpressionType type);
bool isExpressionArithmetic(ExpressionType type);
bool isExpressionStringOperator(ExpressionType type);
bool isExpressionNullComparison(ExpressionType type);
bool isExpressionLeafLiteral(ExpressionType type);
bool isExpressionLeafVariable(ExpressionType type);

ExpressionType comparisonToIDComparison(ExpressionType type);
string expressionTypeToString(ExpressionType type);

} // namespace common
} // namespace graphflow
