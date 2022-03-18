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

    // List functions
    LIST_CREATION = 50,

    // id function
    INTERNAL_ID = 100,
};

static const unordered_map<string, BuiltInFunctionIndices> functionNameToIndicesMap{
    {CAST_TO_DATE_FUNCTION_NAME, CAST_STRING_TO_DATE},
    {CAST_TO_TIMESTAMP_FUNCTION_NAME, CAST_STRING_TO_TIMESTAMP},
    {CAST_TO_INTERVAL_FUNCTION_NAME, CAST_STRING_TO_INTERVAL},
    {LIST_CREATION_FUNC_NAME, LIST_CREATION}, {ID_FUNC_NAME, INTERNAL_ID}};

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
