#pragma once

#include "src/common/include/types.h"

namespace graphflow {
namespace common {

enum ExpressionType {

    // -----------------------------------------
    // Non Leaf Expression
    // -----------------------------------------

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
     * String Operation Expressions
     **/
    STARTS_WITH = 37,
    ENDS_WITH = 38,
    CONTAINS = 39,

    /**
     * Null Operation Expressions
     **/
    IS_NULL = 40,
    IS_NOT_NULL = 41,

    /**
     * Property Expression
     **/
    PROPERTY = 52,

    /**
     * Function Expression
     **/
    FUNCTION = 63,

    // ------------------------------
    // Leaf Expression
    // ------------------------------

    /**
     * Literal Expressions
     **/
    LITERAL_INT = 74,
    LITERAL_DOUBLE = 75,
    LITERAL_STRING = 76,
    LITERAL_BOOLEAN = 77,
    LITERAL_NULL = 78,

    /**
     * Variable Expression
     **/
    VARIABLE = 79,
};

bool isExpressionUnary(ExpressionType type);
bool isExpressionBinary(ExpressionType type);
bool isExpressionLeafLiteral(ExpressionType type);
bool isExpressionLeafVariable(ExpressionType type);
DataType getUnaryExpressionResultDataType(ExpressionType type, DataType operandType);
DataType getBinaryExpressionResultDataType(
    ExpressionType type, DataType leftOperandType, DataType rightOperandType);

} // namespace common
} // namespace graphflow
