#pragma once

#include <cstdint>
#include <string>

using namespace std;

namespace kuzu {
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

// list
const string LIST_CREATION_FUNC_NAME = "LIST_CREATION";
const string LIST_EXTRACT_FUNC_NAME = "LIST_EXTRACT";
const string LIST_ELEMENT_FUNC_NAME = "LIST_ELEMENT";
const string LIST_LEN_FUNC_NAME = "LEN";
const string LIST_CONCAT_FUNC_NAME = "LIST_CONCAT";
const string LIST_CAT_FUNC_NAME = "LIST_CAT";
const string ARRAY_CONCAT_FUNC_NAME = "ARRAY_CONCAT";
const string ARRAY_CAT_FUNC_NAME = "ARRAY_CAT";
const string LIST_APPEND_FUNC_NAME = "LIST_APPEND";
const string ARRAY_APPEND_FUNC_NAME = "ARRAY_APPEND";
const string ARRAY_PUSH_BACK_FUNC_NAME = "ARRAY_PUSH_BACK";
const string LIST_PREPEND_FUNC_NAME = "LIST_PREPEND";
const string ARRAY_PREPEND_FUNC_NAME = "ARRAY_PREPEND";
const string ARRAY_PUSH_FRONT_FUNC_NAME = "ARRAY_PUSH_FRONT";
const string LIST_POSITION_FUNC_NAME = "LIST_POSITION";
const string LIST_INDEXOF_FUNC_NAME = "LIST_INDEXOF";
const string ARRAY_POSITION_FUNC_NAME = "ARRAY_POSITION";
const string ARRAY_INDEXOF_FUNC_NAME = "ARRAY_INDEXOF";
const string LIST_CONTAINS_FUNC_NAME = "LIST_CONTAINS";
const string LIST_HAS_FUNC_NAME = "LIST_HAS";
const string ARRAY_CONTAINS_FUNC_NAME = "ARRAY_CONTAINS";
const string ARRAY_HAS_FUNC_NAME = "ARRAY_HAS";
const string LIST_SLICE_FUNC_NAME = "LIST_SLICE";
const string ARRAY_SLICE_FUNC_NAME = "ARRAY_SLICE";

// comparison
const string EQUALS_FUNC_NAME = "EQUALS";
const string NOT_EQUALS_FUNC_NAME = "NOT_EQUALS";
const string GREATER_THAN_FUNC_NAME = "GREATER_THAN";
const string GREATER_THAN_EQUALS_FUNC_NAME = "GREATER_THAN_EQUALS";
const string LESS_THAN_FUNC_NAME = "LESS_THAN";
const string LESS_THAN_EQUALS_FUNC_NAME = "LESS_THAN_EQUALS";

// arithmetics operators
const string ADD_FUNC_NAME = "+";
const string SUBTRACT_FUNC_NAME = "-";
const string MULTIPLY_FUNC_NAME = "*";
const string DIVIDE_FUNC_NAME = "/";
const string MODULO_FUNC_NAME = "%";
const string POWER_FUNC_NAME = "^";

// arithmetics functions
const string ABS_FUNC_NAME = "ABS";
const string ACOS_FUNC_NAME = "ACOS";
const string ASIN_FUNC_NAME = "ASIN";
const string ATAN_FUNC_NAME = "ATAN";
const string ATAN2_FUNC_NAME = "ATAN2";
const string BITWISE_XOR_FUNC_NAME = "BITWISE_XOR";
const string CBRT_FUNC_NAME = "CBRT";
const string CEIL_FUNC_NAME = "CEIL";
const string CEILING_FUNC_NAME = "CEILING";
const string COS_FUNC_NAME = "COS";
const string COT_FUNC_NAME = "COT";
const string DEGREES_FUNC_NAME = "DEGREES";
const string EVEN_FUNC_NAME = "EVEN";
const string FACTORIAL_FUNC_NAME = "FACTORIAL";
const string FLOOR_FUNC_NAME = "FLOOR";
const string GAMMA_FUNC_NAME = "GAMMA";
const string LGAMMA_FUNC_NAME = "LGAMMA";
const string LN_FUNC_NAME = "LN";
const string LOG_FUNC_NAME = "LOG";
const string LOG2_FUNC_NAME = "LOG2";
const string LOG10_FUNC_NAME = "LOG10";
const string NEGATE_FUNC_NAME = "NEGATE";
const string PI_FUNC_NAME = "PI";
const string POW_FUNC_NAME = "POW";
const string RADIANS_FUNC_NAME = "RADIANS";
const string ROUND_FUNC_NAME = "ROUND";
const string SIN_FUNC_NAME = "SIN";
const string SIGN_FUNC_NAME = "SIGN";
const string SQRT_FUNC_NAME = "SQRT";
const string TAN_FUNC_NAME = "TAN";

// string
const string ARRAY_EXTRACT_FUNC_NAME = "ARRAY_EXTRACT";
const string CONCAT_FUNC_NAME = "CONCAT";
const string CONTAINS_FUNC_NAME = "CONTAINS";
const string ENDS_WITH_FUNC_NAME = "ENDS_WITH";
const string LCASE_FUNC_NAME = "LCASE";
const string LEFT_FUNC_NAME = "LEFT";
const string LENGTH_FUNC_NAME = "LENGTH";
const string LOWER_FUNC_NAME = "LOWER";
const string LPAD_FUNC_NAME = "LPAD";
const string LTRIM_FUNC_NAME = "LTRIM";
const string PREFIX_FUNC_NAME = "PREFIX";
const string REPEAT_FUNC_NAME = "REPEAT";
const string REVERSE_FUNC_NAME = "REVERSE";
const string RIGHT_FUNC_NAME = "RIGHT";
const string RPAD_FUNC_NAME = "RPAD";
const string RTRIM_FUNC_NAME = "RTRIM";
const string STARTS_WITH_FUNC_NAME = "STARTS_WITH";
const string SUBSTR_FUNC_NAME = "SUBSTR";
const string SUBSTRING_FUNC_NAME = "SUBSTRING";
const string SUFFIX_FUNC_NAME = "SUFFIX";
const string TRIM_FUNC_NAME = "TRIM";
const string UCASE_FUNC_NAME = "UCASE";
const string UPPER_FUNC_NAME = "UPPER";

// Date functions.
const string DATE_PART_FUNC_NAME = "DATE_PART";
const string DATEPART_FUNC_NAME = "DATEPART";
const string DATE_TRUNC_FUNC_NAME = "DATE_TRUNC";
const string DATETRUNC_FUNC_NAME = "DATETRUNC";
const string DAYNAME_FUNC_NAME = "DAYNAME";
const string GREATEST_FUNC_NAME = "GREATEST";
const string LAST_DAY_FUNC_NAME = "LAST_DAY";
const string LEAST_FUNC_NAME = "LEAST";
const string MAKE_DATE_FUNC_NAME = "MAKE_DATE";
const string MONTHNAME_FUNC_NAME = "MONTHNAME";

// Timestamp functions.
const string CENTURY_FUNC_NAME = "CENTURY";
const string EPOCH_MS_FUNC_NAME = "EPOCH_MS";
const string TO_TIMESTAMP_FUNC_NAME = "TO_TIMESTAMP";

// Interval functions.
const string TO_YEARS_FUNC_NAME = "TO_YEARS";
const string TO_MONTHS_FUNC_NAME = "TO_MONTHS";
const string TO_DAYS_FUNC_NAME = "TO_DAYS";
const string TO_HOURS_FUNC_NAME = "TO_HOURS";
const string TO_MINUTES_FUNC_NAME = "TO_MINUTES";
const string TO_SECONDS_FUNC_NAME = "TO_SECONDS";
const string TO_MILLISECONDS_FUNC_NAME = "TO_MILLISECONDS";
const string TO_MICROSECONDS_FUNC_NAME = "TO_MICROSECONDS";

const string ID_FUNC_NAME = "ID";

enum ExpressionType : uint8_t {

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
} // namespace kuzu
