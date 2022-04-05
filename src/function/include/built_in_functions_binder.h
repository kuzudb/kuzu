#pragma once

#include <unordered_map>

#include "vector_operations.h"

namespace graphflow {
namespace function {

/**
 * Built-in functions are defined as functions not defined in grammar but instead parsed through
 * "oC_functionName ()". E.g. ABS() is a built-in function.
 */
enum BuiltInFunctionIndices : uint8_t {

    // Cast functions
    CAST_STRING_TO_DATE = 0,
    CAST_STRING_TO_TIMESTAMP = 1,
    CAST_STRING_TO_INTERVAL = 2,
    CAST_TO_STRING = 3,

    // List functions
    LIST_CREATION = 50,

    // id function
    INTERNAL_ID = 100,

    // unary arithmetic functions
    ABS = 150,
    FLOOR = 151,
    CEIL = 152,
    SIN = 153,
    COS = 154,
    TAN = 155,
    COT = 156,
    ASIN = 157,
    ACOS = 158,
    ATAN = 159,
    EVEN = 160,
    FACTORIAL = 161,
    SIGN = 162,
    SQRT = 163,
    CBRT = 164,
    GAMMA = 165,
    LGAMMA = 166,
    LN = 167,
    LOG = 168,
    LOG2 = 169,
    DEGREES = 170,
    RADIANS = 171,
};

static const unordered_map<string, BuiltInFunctionIndices> functionNameToIndicesMap{
    {CAST_TO_DATE_FUNCTION_NAME, CAST_STRING_TO_DATE},
    {CAST_TO_TIMESTAMP_FUNCTION_NAME, CAST_STRING_TO_TIMESTAMP},
    {CAST_TO_INTERVAL_FUNCTION_NAME, CAST_STRING_TO_INTERVAL},
    {CAST_TO_STRING_FUNCTION_NAME, CAST_TO_STRING}, {LIST_CREATION_FUNC_NAME, LIST_CREATION},
    {ID_FUNC_NAME, INTERNAL_ID}, {ABS_FUNC_NAME, ABS}, {FLOOR_FUNC_NAME, FLOOR},
    {CEIL_FUNC_NAME, CEIL}, {SIN_FUNC_NAME, SIN}, {COS_FUNC_NAME, COS}, {TAN_FUNC_NAME, TAN},
    {COT_FUNC_NAME, COT}, {ASIN_FUNC_NAME, ASIN}, {ACOS_FUNC_NAME, ACOS}, {ATAN_FUNC_NAME, ATAN},
    {EVEN_FUNC_NAME, EVEN}, {FACTORIAL_FUNC_NAME, FACTORIAL}, {SIGN_FUNC_NAME, SIGN},
    {SQRT_FUNC_NAME, SQRT}, {CBRT_FUNC_NAME, CBRT}, {GAMMA_FUNC_NAME, GAMMA},
    {LGAMMA_FUNC_NAME, LGAMMA}, {LN_FUNC_NAME, LN}, {LOG_FUNC_NAME, LOG}, {LOG2_FUNC_NAME, LOG2},
    {DEGREES_FUNC_NAME, DEGREES}, {RADIANS_FUNC_NAME, RADIANS}};

class BuiltInFunctionsBinder {

public:
    static pair<scalar_exec_func, DataType> bindExecFunction(
        const string& functionName, const expression_vector& children);

    /**
     * Certain function can be evaluated statically and thus avoid runtime execution.
     * E.g. date("2021-01-01") can be evaluated as date literal statically.
     */
    static bool canApplyStaticEvaluation(
        const string& functionName, const expression_vector& children);

private:
    static void validateFunctionExistence(const string& functionName);
};

} // namespace function
} // namespace graphflow
