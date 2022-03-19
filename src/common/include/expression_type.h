#pragma once

#include <cstdint>
#include <string>

using namespace std;

namespace graphflow {
namespace common {

/**
 * Function name is a temporary identifier used for binder because grammar does not parse built in
 * functions. After binding, expression type should replace function name and used as identifier.
 */
const string COUNT_STAR_FUNC_NAME = "COUNT_STAR";
const string COUNT_FUNC_NAME = "COUNT";
const string SUM_FUNC_NAME = "SUM";
const string AVG_FUNC_NAME = "AVG";
const string MIN_FUNC_NAME = "MIN";
const string MAX_FUNC_NAME = "MAX";

const string CAST_TO_DATE_FUNCTION_NAME = "DATE";
const string CAST_TO_TIMESTAMP_FUNCTION_NAME = "TIMESTAMP";
const string CAST_TO_INTERVAL_FUNCTION_NAME = "INTERVAL";

const string LIST_CREATION_FUNC_NAME = "LIST_CREATION";
const string LIST_EXTRACT_FUNC_NAME = "LIST_EXTRACT";

const string ID_FUNC_NAME = "ID";

const string ABS_FUNC_NAME = "ABS";
const string FLOOR_FUNC_NAME = "FLOOR";
const string CEIL_FUNC_NAME = "CEIL";

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
     * String Operator Expressions
     **/
    STARTS_WITH = 40,
    ENDS_WITH = 41,
    CONTAINS = 42,

    /**
     * List Operator Expressions works only for CSV Line
     */
    LIST_EXTRACT = 46,

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
     * Literal Expressions
     **/
    LITERAL_INT = 80,
    LITERAL_DOUBLE = 81,
    LITERAL_STRING = 82,
    LITERAL_BOOLEAN = 83,
    LITERAL_DATE = 84,
    LITERAL_TIMESTAMP = 85,
    LITERAL_INTERVAL = 86,
    LITERAL_NULL = 87,

    /**
     * Variable Expression
     **/
    VARIABLE = 90,

    /**
     * Cast Expressions
     **/
    FUNCTION = 110,

    /**
     * Aggregation Function Expression
     */
    COUNT_STAR_FUNC = 130,
    COUNT_FUNC = 131,
    SUM_FUNC = 132,
    AVG_FUNC = 133,
    MIN_FUNC = 134,
    MAX_FUNC = 135,

    EXISTENTIAL_SUBQUERY = 190,
};

bool isExpressionUnary(ExpressionType type);
bool isExpressionBinary(ExpressionType type);
bool isExpressionBoolConnection(ExpressionType type);
bool isExpressionComparison(ExpressionType type);
bool isExpressionArithmetic(ExpressionType type);
bool isExpressionStringOperator(ExpressionType type);
bool isExpressionNullOperator(ExpressionType type);
bool isExpressionLiteral(ExpressionType type);
bool isExpressionAggregate(ExpressionType type);
bool isExpressionSubquery(ExpressionType type);

string expressionTypeToString(ExpressionType type);

} // namespace common
} // namespace graphflow
