#pragma once

#include "src/function/include/vector_operations.h"

namespace graphflow {
namespace function {

struct VectorStringOperations : public VectorOperations {};

struct ContainsVectorOperation : public VectorStringOperations {
    static vector<unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct StartsWithVectorOperation : public VectorStringOperations {
    static vector<unique_ptr<VectorOperationDefinition>> getDefinitions();
};

} // namespace function
} // namespace graphflow
