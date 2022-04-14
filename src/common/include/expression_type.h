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
const string CAST_TO_STRING_FUNCTION_NAME = "STRING";

const string LIST_CREATION_FUNC_NAME = "LIST_CREATION";
const string LIST_EXTRACT_FUNC_NAME = "LIST_EXTRACT";

const string ID_FUNC_NAME = "ID";

const string ADD_FUNC_NAME = "ADD";
const string SUBTRACT_FUNC_NAME = "SUBTRACT";
const string MULTIPLY_FUNC_NAME = "MULTIPLY";
const string DIVIDE_FUNC_NAME = "DIVIDE";
const string MODULO_FUNC_NAME = "MODULO";
const string POWER_FUNC_NAME = "POWER";

const string NEGATE_FUNC_NAME = "NEGATE";
const string ABS_FUNC_NAME = "ABS";
const string FLOOR_FUNC_NAME = "FLOOR";
const string CEIL_FUNC_NAME = "CEIL";
const string SIN_FUNC_NAME = "SIN";
const string COS_FUNC_NAME = "COS";
const string TAN_FUNC_NAME = "TAN";
const string COT_FUNC_NAME = "COT";
const string ASIN_FUNC_NAME = "ASIN";
const string ACOS_FUNC_NAME = "ACOS";
const string ATAN_FUNC_NAME = "ATAN";
const string EVEN_FUNC_NAME = "EVEN";
const string FACTORIAL_FUNC_NAME = "FACTORIAL";
const string SIGN_FUNC_NAME = "SIGN";
const string SQRT_FUNC_NAME = "SQRT";
const string CBRT_FUNC_NAME = "CBRT";
const string GAMMA_FUNC_NAME = "GAMMA";
const string LGAMMA_FUNC_NAME = "LGAMMA";
const string LN_FUNC_NAME = "LN";
const string LOG_FUNC_NAME = "LOG";
const string LOG2_FUNC_NAME = "LOG2";
const string DEGREES_FUNC_NAME = "DEGREES";
const string RADIANS_FUNC_NAME = "RADIANS";

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
     * Parameter Expression
     **/
    PARAMETER = 100,

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
