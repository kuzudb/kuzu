#pragma once

#include "function/vector_operations.h"

namespace kuzu {
namespace function {

struct VectorStructOperations : public VectorOperations {
    static void StructPack(const std::vector<std::shared_ptr<common::ValueVector>>& parameters,
        common::ValueVector& result);
};

struct StructPackVectorOperations : public VectorStructOperations {
    static void structPackBindFunc(const binder::expression_vector& arguments,
        FunctionDefinition* definition, common::DataType& actualReturnType);
};

} // namespace function
} // namespace kuzu
