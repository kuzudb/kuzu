#pragma once

#include "common/vector/value_vector.h"
#include "function/vector_operations.h"

namespace kuzu {
namespace function {

struct MapCreationVectorOperations {
    static vector_operation_definitions getDefinitions();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, FunctionDefinition* definition);
};

struct MapExtractVectorOperations {
    static vector_operation_definitions getDefinitions();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, FunctionDefinition* definition);
};

struct MapKeysVectorOperations {
    static vector_operation_definitions getDefinitions();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, FunctionDefinition* definition);
};

struct MapValuesVectorOperations {
    static vector_operation_definitions getDefinitions();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, FunctionDefinition* definition);
};

} // namespace function
} // namespace kuzu
