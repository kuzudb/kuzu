#include "include/vector_list_operations.h"

#include "src/common/types/include/gf_list.h"

namespace graphflow {
namespace function {

static void setGFListAndAllocate(
    ValueVector& vector, uint64_t pos, DataType childType, uint64_t capacity) {
    auto& gfList = ((gf_list_t*)vector.values)[pos];
    gfList.childType = childType;
    gfList.capacity = capacity;
    gfList.size = 0;
    vector.allocateListOverflow(gfList);
}

static void appendToList(gf_list_t& gfList, uint8_t* element, uint64_t elementSize) {
    auto overflowOffset = gfList.size * elementSize;
    switch (gfList.childType) {
    case BOOL:
    case INT64:
    case DOUBLE:
    case DATE:
    case TIMESTAMP:
    case INTERVAL: {
        memcpy(gfList.overflowPtr + overflowOffset, element, elementSize);
    } break;
    default:
        assert(false);
    }
    gfList.size++;
}

void VectorListOperations::ListCreation(
    const vector<shared_ptr<ValueVector>>& parameters, ValueVector& result) {
    result.resetOverflowBuffer();
    assert(!parameters.empty());
    assert(result.dataType == LIST);
    if (result.state->isFlat()) {
        auto pos = result.state->getPositionOfCurrIdx();
        setGFListAndAllocate(result, pos, parameters[0]->dataType, size(parameters));
        for (auto& parameter : parameters) {
            assert(parameter->state->isFlat());
            auto parameterValueOffset =
                parameter->state->getPositionOfCurrIdx() * parameter->getNumBytesPerValue();
            appendToList(((gf_list_t*)result.values)[pos], parameter->values + parameterValueOffset,
                parameter->getNumBytesPerValue());
        }
    } else {
        for (auto i = 0u; i < result.state->selectedSize; ++i) {
            auto pos = result.state->selectedPositions[i];
            setGFListAndAllocate(result, pos, parameters[0]->dataType, size(parameters));
            for (auto& parameter : parameters) {
                auto parameterPos =
                    parameter->state->isFlat() ? parameter->state->getPositionOfCurrIdx() : pos;
                auto parameterValueOffset = parameterPos * parameter->getNumBytesPerValue();
                appendToList(((gf_list_t*)result.values)[pos],
                    parameter->values + parameterValueOffset, parameter->getNumBytesPerValue());
            }
        }
    }
}
} // namespace function
} // namespace graphflow
