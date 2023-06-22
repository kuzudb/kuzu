#pragma once

#include "function/vector_operations.h"

namespace kuzu {
namespace function {

struct OctetLengthVectorOperations : public VectorOperations {
    static vector_operation_definitions getDefinitions();
};

struct EncodeVectorOperations : public VectorOperations {
    static vector_operation_definitions getDefinitions();
};

struct DecodeVectorOperations : public VectorOperations {
    static vector_operation_definitions getDefinitions();
};

} // namespace function
} // namespace kuzu
