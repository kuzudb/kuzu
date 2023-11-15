#pragma once

#include <cstdint>
#include <string>

namespace kuzu {
namespace common {

/**
 * Function name is a temporary identifier used for binder because grammar does not parse built in
 * functions. After binding, expression type should replace function name and used as identifier.
 */
// aggregate
const std::string COUNT_STAR_FUNC_NAME = "COUNT_STAR";
const std::string COUNT_FUNC_NAME = "COUNT";
const std::string SUM_FUNC_NAME = "SUM";
const std::string AVG_FUNC_NAME = "AVG";
const std::string MIN_FUNC_NAME = "MIN";
const std::string MAX_FUNC_NAME = "MAX";
const std::string COLLECT_FUNC_NAME = "COLLECT";

// cast
const std::string CAST_FUNC_NAME = "CAST";
const std::string CAST_DATE_FUNC_NAME = "DATE";
const std::string CAST_TO_DATE_FUNC_NAME = "TO_DATE";
const std::string CAST_TO_TIMESTAMP_FUNC_NAME = "TIMESTAMP";
const std::string CAST_INTERVAL_FUNC_NAME = "INTERVAL";
const std::string CAST_TO_INTERVAL_FUNC_NAME = "TO_INTERVAL";
const std::string CAST_STRING_FUNC_NAME = "STRING";
const std::string CAST_TO_STRING_FUNC_NAME = "TO_STRING";
const std::string CAST_TO_DOUBLE_FUNC_NAME = "TO_DOUBLE";
const std::string CAST_TO_FLOAT_FUNC_NAME = "TO_FLOAT";
const std::string CAST_TO_SERIAL_FUNC_NAME = "TO_SERIAL";
const std::string CAST_TO_INT64_FUNC_NAME = "TO_INT64";
const std::string CAST_TO_INT32_FUNC_NAME = "TO_INT32";
const std::string CAST_TO_INT16_FUNC_NAME = "TO_INT16";
const std::string CAST_TO_INT8_FUNC_NAME = "TO_INT8";
const std::string CAST_TO_UINT64_FUNC_NAME = "TO_UINT64";
const std::string CAST_TO_UINT32_FUNC_NAME = "TO_UINT32";
const std::string CAST_TO_UINT16_FUNC_NAME = "TO_UINT16";
const std::string CAST_TO_UINT8_FUNC_NAME = "TO_UINT8";
const std::string CAST_BLOB_FUNC_NAME = "BLOB";
const std::string CAST_TO_BLOB_FUNC_NAME = "TO_BLOB";
const std::string CAST_TO_BOOL_FUNC_NAME = "TO_BOOL";
const std::string CAST_TO_INT128_FUNC_NAME = "TO_INT128";

// list
const std::string LIST_CREATION_FUNC_NAME = "LIST_CREATION";
const std::string LIST_RANGE_FUNC_NAME = "RANGE";
const std::string LIST_EXTRACT_FUNC_NAME = "LIST_EXTRACT";
const std::string LIST_ELEMENT_FUNC_NAME = "LIST_ELEMENT";
const std::string LIST_CONCAT_FUNC_NAME = "LIST_CONCAT";
const std::string LIST_CAT_FUNC_NAME = "LIST_CAT";
const std::string ARRAY_CONCAT_FUNC_NAME = "ARRAY_CONCAT";
const std::string ARRAY_CAT_FUNC_NAME = "ARRAY_CAT";
const std::string LIST_APPEND_FUNC_NAME = "LIST_APPEND";
const std::string ARRAY_APPEND_FUNC_NAME = "ARRAY_APPEND";
const std::string ARRAY_PUSH_BACK_FUNC_NAME = "ARRAY_PUSH_BACK";
const std::string LIST_PREPEND_FUNC_NAME = "LIST_PREPEND";
const std::string ARRAY_PREPEND_FUNC_NAME = "ARRAY_PREPEND";
const std::string ARRAY_PUSH_FRONT_FUNC_NAME = "ARRAY_PUSH_FRONT";
const std::string LIST_POSITION_FUNC_NAME = "LIST_POSITION";
const std::string LIST_INDEXOF_FUNC_NAME = "LIST_INDEXOF";
const std::string ARRAY_POSITION_FUNC_NAME = "ARRAY_POSITION";
const std::string ARRAY_INDEXOF_FUNC_NAME = "ARRAY_INDEXOF";
const std::string LIST_CONTAINS_FUNC_NAME = "LIST_CONTAINS";
const std::string LIST_HAS_FUNC_NAME = "LIST_HAS";
const std::string ARRAY_CONTAINS_FUNC_NAME = "ARRAY_CONTAINS";
const std::string ARRAY_HAS_FUNC_NAME = "ARRAY_HAS";
const std::string LIST_SLICE_FUNC_NAME = "LIST_SLICE";
const std::string ARRAY_SLICE_FUNC_NAME = "ARRAY_SLICE";
const std::string LIST_SUM_FUNC_NAME = "LIST_SUM";
const std::string LIST_PRODUCT_FUNC_NAME = "LIST_PRODUCT";
const std::string LIST_SORT_FUNC_NAME = "LIST_SORT";
const std::string LIST_REVERSE_SORT_FUNC_NAME = "LIST_REVERSE_SORT";
const std::string LIST_DISTINCT_FUNC_NAME = "LIST_DISTINCT";
const std::string LIST_UNIQUE_FUNC_NAME = "LIST_UNIQUE";
const std::string LIST_ANY_VALUE_FUNC_NAME = "LIST_ANY_VALUE";

// struct
const std::string STRUCT_PACK_FUNC_NAME = "STRUCT_PACK";
const std::string STRUCT_EXTRACT_FUNC_NAME = "STRUCT_EXTRACT";

// map
const std::string MAP_CREATION_FUNC_NAME = "MAP";
const std::string MAP_EXTRACT_FUNC_NAME = "MAP_EXTRACT";
const std::string ELEMENT_AT_FUNC_NAME = "ELEMENT_AT"; // alias of MAP_EXTRACT
const std::string CARDINALITY_FUNC_NAME = "CARDINALITY";
const std::string MAP_KEYS_FUNC_NAME = "MAP_KEYS";
const std::string MAP_VALUES_FUNC_NAME = "MAP_VALUES";

// union
const std::string UNION_VALUE_FUNC_NAME = "UNION_VALUE";
const std::string UNION_TAG_FUNC_NAME = "UNION_TAG";
const std::string UNION_EXTRACT_FUNC_NAME = "UNION_EXTRACT";

// comparison
const std::string EQUALS_FUNC_NAME = "EQUALS";
const std::string NOT_EQUALS_FUNC_NAME = "NOT_EQUALS";
const std::string GREATER_THAN_FUNC_NAME = "GREATER_THAN";
const std::string GREATER_THAN_EQUALS_FUNC_NAME = "GREATER_THAN_EQUALS";
const std::string LESS_THAN_FUNC_NAME = "LESS_THAN";
const std::string LESS_THAN_EQUALS_FUNC_NAME = "LESS_THAN_EQUALS";

// arithmetics operators
const std::string ADD_FUNC_NAME = "+";
const std::string SUBTRACT_FUNC_NAME = "-";
const std::string MULTIPLY_FUNC_NAME = "*";
const std::string DIVIDE_FUNC_NAME = "/";
const std::string MODULO_FUNC_NAME = "%";
const std::string POWER_FUNC_NAME = "^";

// arithmetics functions
const std::string ABS_FUNC_NAME = "ABS";
const std::string ACOS_FUNC_NAME = "ACOS";
const std::string ASIN_FUNC_NAME = "ASIN";
const std::string ATAN_FUNC_NAME = "ATAN";
const std::string ATAN2_FUNC_NAME = "ATAN2";
const std::string BITWISE_XOR_FUNC_NAME = "BITWISE_XOR";
const std::string BITWISE_AND_FUNC_NAME = "BITWISE_AND";
const std::string BITWISE_OR_FUNC_NAME = "BITWISE_OR";
const std::string BITSHIFT_LEFT_FUNC_NAME = "BITSHIFT_LEFT";
const std::string BITSHIFT_RIGHT_FUNC_NAME = "BITSHIFT_RIGHT";
const std::string CBRT_FUNC_NAME = "CBRT";
const std::string CEIL_FUNC_NAME = "CEIL";
const std::string CEILING_FUNC_NAME = "CEILING";
const std::string COS_FUNC_NAME = "COS";
const std::string COT_FUNC_NAME = "COT";
const std::string DEGREES_FUNC_NAME = "DEGREES";
const std::string EVEN_FUNC_NAME = "EVEN";
const std::string FACTORIAL_FUNC_NAME = "FACTORIAL";
const std::string FLOOR_FUNC_NAME = "FLOOR";
const std::string GAMMA_FUNC_NAME = "GAMMA";
const std::string LGAMMA_FUNC_NAME = "LGAMMA";
const std::string LN_FUNC_NAME = "LN";
const std::string LOG_FUNC_NAME = "LOG";
const std::string LOG2_FUNC_NAME = "LOG2";
const std::string LOG10_FUNC_NAME = "LOG10";
const std::string NEGATE_FUNC_NAME = "NEGATE";
const std::string PI_FUNC_NAME = "PI";
const std::string POW_FUNC_NAME = "POW";
const std::string RADIANS_FUNC_NAME = "RADIANS";
const std::string ROUND_FUNC_NAME = "ROUND";
const std::string SIN_FUNC_NAME = "SIN";
const std::string SIGN_FUNC_NAME = "SIGN";
const std::string SQRT_FUNC_NAME = "SQRT";
const std::string TAN_FUNC_NAME = "TAN";

// string
const std::string ARRAY_EXTRACT_FUNC_NAME = "ARRAY_EXTRACT";
const std::string CONCAT_FUNC_NAME = "CONCAT";
const std::string CONTAINS_FUNC_NAME = "CONTAINS";
const std::string ENDS_WITH_FUNC_NAME = "ENDS_WITH";
const std::string LCASE_FUNC_NAME = "LCASE";
const std::string LEFT_FUNC_NAME = "LEFT";
const std::string LENGTH_FUNC_NAME = "LENGTH";
const std::string LOWER_FUNC_NAME = "LOWER";
const std::string LPAD_FUNC_NAME = "LPAD";
const std::string LTRIM_FUNC_NAME = "LTRIM";
const std::string PREFIX_FUNC_NAME = "PREFIX";
const std::string REPEAT_FUNC_NAME = "REPEAT";
const std::string REVERSE_FUNC_NAME = "REVERSE";
const std::string RIGHT_FUNC_NAME = "RIGHT";
const std::string RPAD_FUNC_NAME = "RPAD";
const std::string RTRIM_FUNC_NAME = "RTRIM";
const std::string STARTS_WITH_FUNC_NAME = "STARTS_WITH";
const std::string SUBSTR_FUNC_NAME = "SUBSTR";
const std::string SUBSTRING_FUNC_NAME = "SUBSTRING";
const std::string SUFFIX_FUNC_NAME = "SUFFIX";
const std::string TRIM_FUNC_NAME = "TRIM";
const std::string UCASE_FUNC_NAME = "UCASE";
const std::string UPPER_FUNC_NAME = "UPPER";
const std::string REGEXP_FULL_MATCH_FUNC_NAME = "REGEXP_FULL_MATCH";
const std::string REGEXP_MATCHES_FUNC_NAME = "REGEXP_MATCHES";
const std::string REGEXP_REPLACE_FUNC_NAME = "REGEXP_REPLACE";
const std::string REGEXP_EXTRACT_FUNC_NAME = "REGEXP_EXTRACT";
const std::string REGEXP_EXTRACT_ALL_FUNC_NAME = "REGEXP_EXTRACT_ALL";
const std::string SIZE_FUNC_NAME = "SIZE";

// Date functions.
const std::string DATE_PART_FUNC_NAME = "DATE_PART";
const std::string DATEPART_FUNC_NAME = "DATEPART";
const std::string DATE_TRUNC_FUNC_NAME = "DATE_TRUNC";
const std::string DATETRUNC_FUNC_NAME = "DATETRUNC";
const std::string DAYNAME_FUNC_NAME = "DAYNAME";
const std::string GREATEST_FUNC_NAME = "GREATEST";
const std::string LAST_DAY_FUNC_NAME = "LAST_DAY";
const std::string LEAST_FUNC_NAME = "LEAST";
const std::string MAKE_DATE_FUNC_NAME = "MAKE_DATE";
const std::string MONTHNAME_FUNC_NAME = "MONTHNAME";

// Timestamp functions.
const std::string CENTURY_FUNC_NAME = "CENTURY";
const std::string EPOCH_MS_FUNC_NAME = "EPOCH_MS";
const std::string TO_TIMESTAMP_FUNC_NAME = "TO_TIMESTAMP";

// Interval functions.
const std::string TO_YEARS_FUNC_NAME = "TO_YEARS";
const std::string TO_MONTHS_FUNC_NAME = "TO_MONTHS";
const std::string TO_DAYS_FUNC_NAME = "TO_DAYS";
const std::string TO_HOURS_FUNC_NAME = "TO_HOURS";
const std::string TO_MINUTES_FUNC_NAME = "TO_MINUTES";
const std::string TO_SECONDS_FUNC_NAME = "TO_SECONDS";
const std::string TO_MILLISECONDS_FUNC_NAME = "TO_MILLISECONDS";
const std::string TO_MICROSECONDS_FUNC_NAME = "TO_MICROSECONDS";

// Node/Rel functions.
const std::string ID_FUNC_NAME = "ID";
const std::string LABEL_FUNC_NAME = "LABEL";
const std::string OFFSET_FUNC_NAME = "OFFSET";

// Path functions
const std::string NODES_FUNC_NAME = "NODES";
const std::string RELS_FUNC_NAME = "RELS";
const std::string PROPERTIES_FUNC_NAME = "PROPERTIES";
const std::string IS_TRAIL_FUNC_NAME = "IS_TRAIL";
const std::string IS_ACYCLIC_FUNC_NAME = "IS_ACYCLIC";

// Blob functions
const std::string OCTET_LENGTH_FUNC_NAME = "OCTET_LENGTH";
const std::string ENCODE_FUNC_NAME = "ENCODE";
const std::string DECODE_FUNC_NAME = "DECODE";

// TABLE functions
const std::string TABLE_INFO_FUNC_NAME = "TABLE_INFO";
const std::string DB_VERSION_FUNC_NAME = "DB_VERSION";
const std::string CURRENT_SETTING_FUNC_NAME = "CURRENT_SETTING";
const std::string SHOW_TABLES_FUNC_NAME = "SHOW_TABLES";
const std::string SHOW_CONNECTION_FUNC_NAME = "SHOW_CONNECTION";
const std::string READ_PARQUET_FUNC_NAME = "READ_PARQUET";
const std::string READ_NPY_FUNC_NAME = "READ_NPY";
const std::string READ_CSV_SERIAL_FUNC_NAME = "READ_CSV_SERIAL";
const std::string READ_CSV_PARALLEL_FUNC_NAME = "READ_CSV_PARALLEL";
const std::string READ_RDF_FUNC_NAME = "READ_RDF";
const std::string READ_PANDAS_FUNC_NAME = "READ_PANDAS";

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

    MACRO = 210,
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
