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
// aggregate
const string COUNT_STAR_FUNC_NAME = "COUNT_STAR";
const string COUNT_FUNC_NAME = "COUNT";
const string SUM_FUNC_NAME = "SUM";
const string AVG_FUNC_NAME = "AVG";
const string MIN_FUNC_NAME = "MIN";
const string MAX_FUNC_NAME = "MAX";

// cast
const string CAST_TO_DATE_FUNC_NAME = "DATE";
const string CAST_TO_TIMESTAMP_FUNC_NAME = "TIMESTAMP";
const string CAST_TO_INTERVAL_FUNC_NAME = "INTERVAL";
const string CAST_TO_STRING_FUNC_NAME = "STRING";
const string IMPLICIT_CAST_TO_BOOL_FUNC_NAME = "_BOOL";
const string IMPLICIT_CAST_TO_INT_FUNC_NAME = "_INT";
const string IMPLICIT_CAST_TO_STRING_FUNC_NAME = "_STRING";
const string IMPLICIT_CAST_TO_DATE_FUNC_NAME = "_DATE";
const string IMPLICIT_CAST_TO_TIMESTAMP_FUNC_NAME = "_TIMESTAMP";
const string IMPLICIT_CAST_TO_UNSTRUCTURED_FUNC_NAME = "_UNSTRUCTURED";

// list
const string LIST_CREATION_FUNC_NAME = "LIST_CREATION";
const string LIST_EXTRACT_FUNC_NAME = "LIST_EXTRACT";

// comparison
const string EQUALS_FUNC_NAME = "EQUALS";
const string NOT_EQUALS_FUNC_NAME = "NOT_EQUALS";
const string GREATER_THAN_FUNC_NAME = "GREATER_THAN";
const string GREATER_THAN_EQUALS_FUNC_NAME = "GREATER_THAN_EQUALS";
const string LESS_THAN_FUNC_NAME = "LESS_THAN";
const string LESS_THAN_EQUALS_FUNC_NAME = "LESS_THAN_EQUALS";

// arithmetics
const string ADD_FUNC_NAME = "+";
const string SUBTRACT_FUNC_NAME = "-";
const string MULTIPLY_FUNC_NAME = "*";
const string DIVIDE_FUNC_NAME = "/";
const string MODULO_FUNC_NAME = "%";
const string POWER_FUNC_NAME = "^";
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

// string
const string CONTAINS_FUNC_NAME = "CONTAINS";
const string STARTS_WITH_FUNC_NAME = "STARTS_WITH";
const string ENDS_WITH_FUNC_NAME = "ENDS_WITH";

// Date functions.
const string DAYNAME_FUNC_NAME = "DAYNAME";
const string MONTHNAME_FUNC_NAME = "MONTHNAME";
const string LAST_DAY_FUNC_NAME = "LAST_DAY";
const string DATE_PART_FUNC_NAME = "DATE_PART";
const string DATE_TRUNC_FUNC_NAME = "DATE_TRUNC";
const string GREATEST_FUNC_NAME = "GREATEST";
const string LEAST_FUNC_NAME = "LEAST";
const string CENTURY_FUNC_NAME = "CENTURY";
const string EPOCH_MS_FUNC_NAME = "EPOCH_MS";
const string TO_TIMESTAMP_FUNC_NAME = "TO_TIMESTAMP";
const string TO_YEARS_FUNC_NAME = "TO_YEARS";
const string TO_MONTHS_FUNC_NAME = "TO_MONTHS";
const string TO_DAYS_FUNC_NAME = "TO_DAYS";
const string TO_HOURS_FUNC_NAME = "TO_HOURS";
const string TO_MINUTES_FUNC_NAME = "TO_MINUTES";
const string TO_SECONDS_FUNC_NAME = "TO_SECONDS";
const string TO_MILLISECONDS_FUNC_NAME = "TO_MILLISECONDS";
const string TO_MICROSECONDS_NAME = "TO_MICROSECONDS";

const string ID_FUNC_NAME = "ID";

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

    VARIABLE = 90,

    PARAMETER = 100,

    FUNCTION = 110,

    AGGREGATE_FUNCTION = 130,

    EXISTENTIAL_SUBQUERY = 190,
};

bool isExpressionUnary(ExpressionType type);
bool isExpressionBinary(ExpressionType type);
bool isExpressionBoolConnection(ExpressionType type);
bool isExpressionComparison(ExpressionType type);
bool isExpressionNullOperator(ExpressionType type);
bool isExpressionLiteral(ExpressionType type);
bool isExpressionAggregate(ExpressionType type);
bool isExpressionSubquery(ExpressionType type);

string expressionTypeToString(ExpressionType type);

} // namespace common
} // namespace graphflow
