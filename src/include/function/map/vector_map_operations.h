#pragma once

#include "common/vector/value_vector_utils.h"
#include "function/vector_operations.h"

namespace kuzu {
namespace function {

struct MapVectorOperations {
    static vector_operation_definitions getDefinitions();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, FunctionDefinition* definition);
};

} // namespace function
} // namespace kuzu
