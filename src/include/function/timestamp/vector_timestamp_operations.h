#pragma once

#include "function/vector_operations.h"

namespace kuzu {
namespace function {
class VectorTimestampOperations : public VectorOperations {};

struct CenturyVectorOperation : public VectorTimestampOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct EpochMsVectorOperation : public VectorTimestampOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct ToTimestampVectorOperation : public VectorTimestampOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
};

} // namespace function
} // namespace kuzu
