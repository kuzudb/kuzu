#pragma once

#include "src/function/include/vector_operations.h"

using namespace graphflow::common;

namespace graphflow {
namespace function {

class VectorListOperations : public VectorOperations {

public:
    static void ListCreation(
        const vector<shared_ptr<ValueVector>>& parameters, ValueVector& result);
};

struct ListCreationVectorOperation : public VectorListOperations {
    static vector<unique_ptr<VectorOperationDefinition>> getDefinitions();
};

} // namespace function
} // namespace graphflow
