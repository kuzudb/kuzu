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
const string ABS_FUNC_NAME = "ABS";
const string COUNT_STAR_FUNC_NAME = "COUNT_STAR";
const string COUNT_FUNC_NAME = "COUNT";
const string SUM_FUNC_NAME = "SUM";
const string AVG_FUNC_NAME = "AVG";
const string MIN_FUNC_NAME = "MIN";
const string MAX_FUNC_NAME = "MAX";
const string ID_FUNC_NAME = "ID";
const string DATE_FUNC_NAME = "DATE";
const string TIMESTAMP_FUNC_NAME = "TIMESTAMP";
const string FLOOR_FUNC_NAME = "FLOOR";
const string CEIL_FUNC_NAME = "CEIL";
const string INTERVAL_FUNC_NAME = "INTERVAL";

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
    CAST_TO_STRING = 100,
    CAST_TO_UNSTRUCTURED_VALUE = 101,
    CAST_UNSTRUCTURED_TO_BOOL_VALUE = 102,
    CAST_STRING_TO_DATE = 103,
    CAST_STRING_TO_TIMESTAMP = 104,
    CAST_STRING_TO_INTERVAL = 105,
    FUNCTION = 110,

    /**
     * Arithmetic Function Expression
     **/
    ABS_FUNC = 111,
    FLOOR_FUNC = 112,
    CEIL_FUNC = 113,

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
bool isExpressionIDComparison(ExpressionType type);
bool isExpressionArithmetic(ExpressionType type);
bool isExpressionStringOperator(ExpressionType type);
bool isExpressionNullComparison(ExpressionType type);
bool isExpressionLiteral(ExpressionType type);
bool isExpressionAggregate(ExpressionType type);

ExpressionType comparisonToIDComparison(ExpressionType type);
string expressionTypeToString(ExpressionType type);

} // namespace common
} // namespace graphflow
