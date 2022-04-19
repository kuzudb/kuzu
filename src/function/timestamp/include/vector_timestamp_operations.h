#pragma once

#include "src/function/include/vector_operations.h"

namespace graphflow {
namespace function {
class VectorTimestampOperations : public VectorOperations {};

struct CenturyVectorOperation : public VectorTimestampOperations {
    static vector<unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct EpochMsVectorOperation : public VectorTimestampOperations {
    static vector<unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct ToTimestampVectorOperation : public VectorTimestampOperations {
    static vector<unique_ptr<VectorOperationDefinition>> getDefinitions();
};

} // namespace function
} // namespace graphflow
