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
const char* const COUNT_STAR_FUNC_NAME = "COUNT_STAR";
const char* const COUNT_FUNC_NAME = "COUNT";
const char* const SUM_FUNC_NAME = "SUM";
const char* const AVG_FUNC_NAME = "AVG";
const char* const MIN_FUNC_NAME = "MIN";
const char* const MAX_FUNC_NAME = "MAX";
const char* const COLLECT_FUNC_NAME = "COLLECT";

// cast
const char* const CAST_FUNC_NAME = "CAST";
const char* const CAST_DATE_FUNC_NAME = "DATE";
const char* const CAST_TO_DATE_FUNC_NAME = "TO_DATE";
const char* const CAST_TO_TIMESTAMP_FUNC_NAME = "TIMESTAMP";
const char* const CAST_INTERVAL_FUNC_NAME = "INTERVAL";
const char* const CAST_TO_INTERVAL_FUNC_NAME = "TO_INTERVAL";
const char* const CAST_STRING_FUNC_NAME = "STRING";
const char* const CAST_TO_STRING_FUNC_NAME = "TO_STRING";
const char* const CAST_TO_DOUBLE_FUNC_NAME = "TO_DOUBLE";
const char* const CAST_TO_FLOAT_FUNC_NAME = "TO_FLOAT";
const char* const CAST_TO_SERIAL_FUNC_NAME = "TO_SERIAL";
const char* const CAST_TO_INT64_FUNC_NAME = "TO_INT64";
const char* const CAST_TO_INT32_FUNC_NAME = "TO_INT32";
const char* const CAST_TO_INT16_FUNC_NAME = "TO_INT16";
const char* const CAST_TO_INT8_FUNC_NAME = "TO_INT8";
const char* const CAST_TO_UINT64_FUNC_NAME = "TO_UINT64";
const char* const CAST_TO_UINT32_FUNC_NAME = "TO_UINT32";
const char* const CAST_TO_UINT16_FUNC_NAME = "TO_UINT16";
const char* const CAST_TO_UINT8_FUNC_NAME = "TO_UINT8";
const char* const CAST_BLOB_FUNC_NAME = "BLOB";
const char* const CAST_TO_BLOB_FUNC_NAME = "TO_BLOB";
const char* const CAST_UUID_FUNC_NAME = "UUID";
const char* const CAST_TO_UUID_FUNC_NAME = "TO_UUID";
const char* const CAST_TO_BOOL_FUNC_NAME = "TO_BOOL";
const char* const CAST_TO_INT128_FUNC_NAME = "TO_INT128";

// list
const char* const LIST_CREATION_FUNC_NAME = "LIST_CREATION";
const char* const LIST_RANGE_FUNC_NAME = "RANGE";
const char* const LIST_EXTRACT_FUNC_NAME = "LIST_EXTRACT";
const char* const LIST_ELEMENT_FUNC_NAME = "LIST_ELEMENT";
const char* const LIST_CONCAT_FUNC_NAME = "LIST_CONCAT";
const char* const LIST_CAT_FUNC_NAME = "LIST_CAT";
const char* const ARRAY_CONCAT_FUNC_NAME = "ARRAY_CONCAT";
const char* const ARRAY_CAT_FUNC_NAME = "ARRAY_CAT";
const char* const LIST_APPEND_FUNC_NAME = "LIST_APPEND";
const char* const ARRAY_APPEND_FUNC_NAME = "ARRAY_APPEND";
const char* const ARRAY_PUSH_BACK_FUNC_NAME = "ARRAY_PUSH_BACK";
const char* const LIST_PREPEND_FUNC_NAME = "LIST_PREPEND";
const char* const ARRAY_PREPEND_FUNC_NAME = "ARRAY_PREPEND";
const char* const ARRAY_PUSH_FRONT_FUNC_NAME = "ARRAY_PUSH_FRONT";
const char* const LIST_POSITION_FUNC_NAME = "LIST_POSITION";
const char* const LIST_INDEXOF_FUNC_NAME = "LIST_INDEXOF";
const char* const ARRAY_POSITION_FUNC_NAME = "ARRAY_POSITION";
const char* const ARRAY_INDEXOF_FUNC_NAME = "ARRAY_INDEXOF";
const char* const LIST_CONTAINS_FUNC_NAME = "LIST_CONTAINS";
const char* const LIST_HAS_FUNC_NAME = "LIST_HAS";
const char* const ARRAY_CONTAINS_FUNC_NAME = "ARRAY_CONTAINS";
const char* const ARRAY_HAS_FUNC_NAME = "ARRAY_HAS";
const char* const LIST_SLICE_FUNC_NAME = "LIST_SLICE";
const char* const ARRAY_SLICE_FUNC_NAME = "ARRAY_SLICE";
const char* const LIST_SUM_FUNC_NAME = "LIST_SUM";
const char* const LIST_PRODUCT_FUNC_NAME = "LIST_PRODUCT";
const char* const LIST_SORT_FUNC_NAME = "LIST_SORT";
const char* const LIST_REVERSE_SORT_FUNC_NAME = "LIST_REVERSE_SORT";
const char* const LIST_DISTINCT_FUNC_NAME = "LIST_DISTINCT";
const char* const LIST_UNIQUE_FUNC_NAME = "LIST_UNIQUE";
const char* const LIST_ANY_VALUE_FUNC_NAME = "LIST_ANY_VALUE";

// struct
const char* const STRUCT_PACK_FUNC_NAME = "STRUCT_PACK";
const char* const STRUCT_EXTRACT_FUNC_NAME = "STRUCT_EXTRACT";

// map
const char* const MAP_CREATION_FUNC_NAME = "MAP";
const char* const MAP_EXTRACT_FUNC_NAME = "MAP_EXTRACT";
const char* const ELEMENT_AT_FUNC_NAME = "ELEMENT_AT"; // alias of MAP_EXTRACT
const char* const CARDINALITY_FUNC_NAME = "CARDINALITY";
const char* const MAP_KEYS_FUNC_NAME = "MAP_KEYS";
const char* const MAP_VALUES_FUNC_NAME = "MAP_VALUES";

// union
const char* const UNION_VALUE_FUNC_NAME = "UNION_VALUE";
const char* const UNION_TAG_FUNC_NAME = "UNION_TAG";
const char* const UNION_EXTRACT_FUNC_NAME = "UNION_EXTRACT";

// comparison
const char* const EQUALS_FUNC_NAME = "EQUALS";
const char* const NOT_EQUALS_FUNC_NAME = "NOT_EQUALS";
const char* const GREATER_THAN_FUNC_NAME = "GREATER_THAN";
const char* const GREATER_THAN_EQUALS_FUNC_NAME = "GREATER_THAN_EQUALS";
const char* const LESS_THAN_FUNC_NAME = "LESS_THAN";
const char* const LESS_THAN_EQUALS_FUNC_NAME = "LESS_THAN_EQUALS";

// arithmetics operators
const char* const ADD_FUNC_NAME = "+";
const char* const SUBTRACT_FUNC_NAME = "-";
const char* const MULTIPLY_FUNC_NAME = "*";
const char* const DIVIDE_FUNC_NAME = "/";
const char* const MODULO_FUNC_NAME = "%";
const char* const POWER_FUNC_NAME = "^";

// arithmetics functions
const char* const ABS_FUNC_NAME = "ABS";
const char* const ACOS_FUNC_NAME = "ACOS";
const char* const ASIN_FUNC_NAME = "ASIN";
const char* const ATAN_FUNC_NAME = "ATAN";
const char* const ATAN2_FUNC_NAME = "ATAN2";
const char* const BITWISE_XOR_FUNC_NAME = "BITWISE_XOR";
const char* const BITWISE_AND_FUNC_NAME = "BITWISE_AND";
const char* const BITWISE_OR_FUNC_NAME = "BITWISE_OR";
const char* const BITSHIFT_LEFT_FUNC_NAME = "BITSHIFT_LEFT";
const char* const BITSHIFT_RIGHT_FUNC_NAME = "BITSHIFT_RIGHT";
const char* const CBRT_FUNC_NAME = "CBRT";
const char* const CEIL_FUNC_NAME = "CEIL";
const char* const CEILING_FUNC_NAME = "CEILING";
const char* const COS_FUNC_NAME = "COS";
const char* const COT_FUNC_NAME = "COT";
const char* const DEGREES_FUNC_NAME = "DEGREES";
const char* const EVEN_FUNC_NAME = "EVEN";
const char* const FACTORIAL_FUNC_NAME = "FACTORIAL";
const char* const FLOOR_FUNC_NAME = "FLOOR";
const char* const GAMMA_FUNC_NAME = "GAMMA";
const char* const LGAMMA_FUNC_NAME = "LGAMMA";
const char* const LN_FUNC_NAME = "LN";
const char* const LOG_FUNC_NAME = "LOG";
const char* const LOG2_FUNC_NAME = "LOG2";
const char* const LOG10_FUNC_NAME = "LOG10";
const char* const NEGATE_FUNC_NAME = "NEGATE";
const char* const PI_FUNC_NAME = "PI";
const char* const POW_FUNC_NAME = "POW";
const char* const RADIANS_FUNC_NAME = "RADIANS";
const char* const ROUND_FUNC_NAME = "ROUND";
const char* const SIN_FUNC_NAME = "SIN";
const char* const SIGN_FUNC_NAME = "SIGN";
const char* const SQRT_FUNC_NAME = "SQRT";
const char* const TAN_FUNC_NAME = "TAN";

// string
const char* const ARRAY_EXTRACT_FUNC_NAME = "ARRAY_EXTRACT";
const char* const CONCAT_FUNC_NAME = "CONCAT";
const char* const CONTAINS_FUNC_NAME = "CONTAINS";
const char* const ENDS_WITH_FUNC_NAME = "ENDS_WITH";
const char* const LCASE_FUNC_NAME = "LCASE";
const char* const LEFT_FUNC_NAME = "LEFT";
const char* const LENGTH_FUNC_NAME = "LENGTH";
const char* const LOWER_FUNC_NAME = "LOWER";
const char* const LPAD_FUNC_NAME = "LPAD";
const char* const LTRIM_FUNC_NAME = "LTRIM";
const char* const PREFIX_FUNC_NAME = "PREFIX";
const char* const REPEAT_FUNC_NAME = "REPEAT";
const char* const REVERSE_FUNC_NAME = "REVERSE";
const char* const RIGHT_FUNC_NAME = "RIGHT";
const char* const RPAD_FUNC_NAME = "RPAD";
const char* const RTRIM_FUNC_NAME = "RTRIM";
const char* const STARTS_WITH_FUNC_NAME = "STARTS_WITH";
const char* const SUBSTR_FUNC_NAME = "SUBSTR";
const char* const SUBSTRING_FUNC_NAME = "SUBSTRING";
const char* const SUFFIX_FUNC_NAME = "SUFFIX";
const char* const TRIM_FUNC_NAME = "TRIM";
const char* const UCASE_FUNC_NAME = "UCASE";
const char* const UPPER_FUNC_NAME = "UPPER";
const char* const REGEXP_FULL_MATCH_FUNC_NAME = "REGEXP_FULL_MATCH";
const char* const REGEXP_MATCHES_FUNC_NAME = "REGEXP_MATCHES";
const char* const REGEXP_REPLACE_FUNC_NAME = "REGEXP_REPLACE";
const char* const REGEXP_EXTRACT_FUNC_NAME = "REGEXP_EXTRACT";
const char* const REGEXP_EXTRACT_ALL_FUNC_NAME = "REGEXP_EXTRACT_ALL";
const char* const SIZE_FUNC_NAME = "SIZE";

// Date functions.
const char* const DATE_PART_FUNC_NAME = "DATE_PART";
const char* const DATEPART_FUNC_NAME = "DATEPART";
const char* const DATE_TRUNC_FUNC_NAME = "DATE_TRUNC";
const char* const DATETRUNC_FUNC_NAME = "DATETRUNC";
const char* const DAYNAME_FUNC_NAME = "DAYNAME";
const char* const GREATEST_FUNC_NAME = "GREATEST";
const char* const LAST_DAY_FUNC_NAME = "LAST_DAY";
const char* const LEAST_FUNC_NAME = "LEAST";
const char* const MAKE_DATE_FUNC_NAME = "MAKE_DATE";
const char* const MONTHNAME_FUNC_NAME = "MONTHNAME";

// Timestamp functions.
const char* const CENTURY_FUNC_NAME = "CENTURY";
const char* const EPOCH_MS_FUNC_NAME = "EPOCH_MS";
const char* const TO_TIMESTAMP_FUNC_NAME = "TO_TIMESTAMP";

// Interval functions.
const char* const TO_YEARS_FUNC_NAME = "TO_YEARS";
const char* const TO_MONTHS_FUNC_NAME = "TO_MONTHS";
const char* const TO_DAYS_FUNC_NAME = "TO_DAYS";
const char* const TO_HOURS_FUNC_NAME = "TO_HOURS";
const char* const TO_MINUTES_FUNC_NAME = "TO_MINUTES";
const char* const TO_SECONDS_FUNC_NAME = "TO_SECONDS";
const char* const TO_MILLISECONDS_FUNC_NAME = "TO_MILLISECONDS";
const char* const TO_MICROSECONDS_FUNC_NAME = "TO_MICROSECONDS";

// Node/Rel functions.
const char* const ID_FUNC_NAME = "ID";
const char* const LABEL_FUNC_NAME = "LABEL";
const char* const OFFSET_FUNC_NAME = "OFFSET";

// Path functions
const char* const NODES_FUNC_NAME = "NODES";
const char* const RELS_FUNC_NAME = "RELS";
const char* const PROPERTIES_FUNC_NAME = "PROPERTIES";
const char* const IS_TRAIL_FUNC_NAME = "IS_TRAIL";
const char* const IS_ACYCLIC_FUNC_NAME = "IS_ACYCLIC";

// Blob functions
const char* const OCTET_LENGTH_FUNC_NAME = "OCTET_LENGTH";
const char* const ENCODE_FUNC_NAME = "ENCODE";
const char* const DECODE_FUNC_NAME = "DECODE";

// UUID functions
const char* const GEN_RANDOM_UUID_FUNC_NAME = "GEN_RANDOM_UUID";

// RDF functions
const char* const TYPE_FUNC_NAME = "TYPE";
const char* const VALIDATE_PREDICATE_FUNC_NAME = "VALIDATE_PREDICATE";

// TABLE functions
const char* const TABLE_INFO_FUNC_NAME = "TABLE_INFO";
const char* const DB_VERSION_FUNC_NAME = "DB_VERSION";
const char* const CURRENT_SETTING_FUNC_NAME = "CURRENT_SETTING";
const char* const SHOW_TABLES_FUNC_NAME = "SHOW_TABLES";
const char* const SHOW_CONNECTION_FUNC_NAME = "SHOW_CONNECTION";
const char* const READ_PARQUET_FUNC_NAME = "READ_PARQUET";
const char* const READ_NPY_FUNC_NAME = "READ_NPY";
const char* const READ_CSV_SERIAL_FUNC_NAME = "READ_CSV_SERIAL";
const char* const READ_CSV_PARALLEL_FUNC_NAME = "READ_CSV_PARALLEL";
const char* const READ_RDF_RESOURCE_FUNC_NAME = "READ_RDF_RESOURCE";
const char* const READ_RDF_LITERAL_FUNC_NAME = "READ_RDF_LITERAL";
const char* const READ_RDF_RESOURCE_TRIPLE_FUNC_NAME = "READ_RDF_RESOURCE_TRIPLE";
const char* const READ_RDF_LITERAL_TRIPLE_FUNC_NAME = "READ_RDF_LITERAL_TRIPLE";
const char* const READ_RDF_ALL_TRIPLE_FUNC_NAME = "READ_RDF_ALL_TRIPLE";
const char* const IN_MEM_READ_RDF_RESOURCE_FUNC_NAME = "IN_MEM_READ_RDF_RESOURCE";
const char* const IN_MEM_READ_RDF_LITERAL_FUNC_NAME = "IN_MEM_READ_RDF_LITERAL";
const char* const IN_MEM_READ_RDF_RESOURCE_TRIPLE_FUNC_NAME = "IN_MEM_READ_RDF_RESOURCE_TRIPLE";
const char* const IN_MEM_READ_RDF_LITERAL_TRIPLE_FUNC_NAME = "IN_MEM_READ_RDF_LITERAL_TRIPLE";
const char* const READ_PANDAS_FUNC_NAME = "READ_PANDAS";
const char* const STORAGE_INFO_FUNC_NAME = "STORAGE_INFO";

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
