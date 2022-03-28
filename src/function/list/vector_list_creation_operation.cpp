#include "include/vector_list_operations.h"

#include "src/common/types/include/gf_list.h"

namespace graphflow {
namespace function {

void VectorListOperations::ListCreation(
    const vector<shared_ptr<ValueVector>>& parameters, ValueVector& result) {
    result.resetOverflowBuffer();
    assert(!parameters.empty() && result.dataType == LIST);
    auto childType = parameters[0]->dataType;
    auto numBytesOfListElement = Types::getDataTypeSize(childType);
    vector<uint8_t*> listElements(parameters.size());
    if (result.state->isFlat()) {
        auto pos = result.state->getPositionOfCurrIdx();
        auto& gfList = ((gf_list_t*)result.values.get())[pos];
        for (auto paramIdx = 0u; paramIdx < parameters.size(); paramIdx++) {
            assert(parameters[paramIdx]->state->isFlat());
            listElements[paramIdx] =
                parameters[paramIdx]->values.get() + pos * numBytesOfListElement;
        }
        TypeUtils::copyList(childType, listElements, gfList, *result.overflowBuffer);
    } else {
        for (auto selectedPos = 0u; selectedPos < result.state->selectedSize; ++selectedPos) {
            auto pos = result.state->selectedPositions[selectedPos];
            auto& gfList = ((gf_list_t*)result.values.get())[pos];
            for (auto paramIdx = 0u; paramIdx < parameters.size(); paramIdx++) {
                auto parameterPos = parameters[paramIdx]->state->isFlat() ?
                                        parameters[paramIdx]->state->getPositionOfCurrIdx() :
                                        pos;
                listElements[paramIdx] =
                    parameters[paramIdx]->values.get() + parameterPos * numBytesOfListElement;
            }
            TypeUtils::copyList(childType, listElements, gfList, *result.overflowBuffer);
        }
    }
}
} // namespace function
} // namespace graphflow
