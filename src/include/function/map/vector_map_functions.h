#pragma once

#include "common/vector/value_vector.h"
#include "function/vector_functions.h"

namespace kuzu {
namespace function {

struct MapCreationVectorFunctions {
    static vector_function_definitions getDefinitions();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, FunctionDefinition* definition);
};

struct MapExtractVectorFunctions {
    static vector_function_definitions getDefinitions();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, FunctionDefinition* definition);
};

struct MapKeysVectorFunctions {
    static vector_function_definitions getDefinitions();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, FunctionDefinition* definition);
};

struct MapValuesVectorFunctions {
    static vector_function_definitions getDefinitions();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, FunctionDefinition* definition);
};

} // namespace function
} // namespace kuzu
