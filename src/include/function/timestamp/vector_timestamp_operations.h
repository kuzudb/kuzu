#pragma once

#include "function/vector_operations.h"

namespace kuzu {
namespace function {
class VectorTimestampOperations : public VectorOperations {};

struct CenturyVectorOperation : public VectorTimestampOperations {
    static vector_operation_definitions getDefinitions();
};

struct EpochMsVectorOperation : public VectorTimestampOperations {
    static vector_operation_definitions getDefinitions();
};

struct ToTimestampVectorOperation : public VectorTimestampOperations {
    static vector_operation_definitions getDefinitions();
};

} // namespace function
} // namespace kuzu
