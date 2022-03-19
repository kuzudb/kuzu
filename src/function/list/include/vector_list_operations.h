#pragma once

#include "src/function/include/vector_operations.h"

using namespace graphflow::common;

namespace graphflow {
namespace function {

class VectorListOperations : public VectorOperations {

public:
    static pair<scalar_exec_func, DataType> bindListCreationExecFunction(
        const expression_vector& children) {
        if (!children.empty()) {
            auto expectedDataType = children[0]->dataType;
            for (auto& child : children) {
                validateParameterType(LIST_CREATION_FUNC_NAME, *child, expectedDataType);
            }
        }
        return make_pair(ListCreation, LIST);
    }

private:
    static void ListCreation(
        const vector<shared_ptr<ValueVector>>& parameters, ValueVector& result);
};

} // namespace function
} // namespace graphflow
