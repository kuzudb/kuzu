#include "include/vector_list_operations.h"

#include "src/common/types/include/gf_list.h"

namespace graphflow {
namespace function {

void VectorListOperations::ListCreation(
    const vector<shared_ptr<ValueVector>>& parameters, ValueVector& result) {
    result.resetOverflowBuffer();
    assert(!parameters.empty() && result.dataType.typeID == LIST);
    auto& childType = parameters[0]->dataType;
    auto numBytesOfListElement = Types::getDataTypeSize(childType);
    vector<uint8_t*> listElements(parameters.size());
    if (result.state->isFlat()) {
        auto pos = result.state->getPositionOfCurrIdx();
        auto& gfList = ((gf_list_t*)result.values)[pos];
        for (auto paramIdx = 0u; paramIdx < parameters.size(); paramIdx++) {
            assert(parameters[paramIdx]->state->isFlat());
            listElements[paramIdx] = parameters[paramIdx]->values + pos * numBytesOfListElement;
        }
        TypeUtils::copyListNonRecursive(
            listElements, gfList, result.dataType, result.getOverflowBuffer());
    } else {
        for (auto selectedPos = 0u; selectedPos < result.state->selectedSize; ++selectedPos) {
            auto pos = result.state->selectedPositions[selectedPos];
            auto& gfList = ((gf_list_t*)result.values)[pos];
            for (auto paramIdx = 0u; paramIdx < parameters.size(); paramIdx++) {
                auto parameterPos = parameters[paramIdx]->state->isFlat() ?
                                        parameters[paramIdx]->state->getPositionOfCurrIdx() :
                                        pos;
                listElements[paramIdx] =
                    parameters[paramIdx]->values + parameterPos * numBytesOfListElement;
            }
            TypeUtils::copyListNonRecursive(
                listElements, gfList, result.dataType, result.getOverflowBuffer());
        }
    }
}

vector<unique_ptr<VectorOperationDefinition>> ListCreationVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> result;
    for (auto& dataTypeID :
        vector<DataTypeID>{BOOL, INT64, DOUBLE, STRING, DATE, TIMESTAMP, INTERVAL, LIST}) {
        result.push_back(make_unique<VectorOperationDefinition>(LIST_CREATION_FUNC_NAME,
            vector<DataTypeID>{dataTypeID}, LIST, ListCreation, true /* isVarlength*/));
    }
    return result;
}

} // namespace function
} // namespace graphflow
