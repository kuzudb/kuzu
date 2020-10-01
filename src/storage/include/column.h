#ifndef GRAPHFLOW_STORAGE_COLUMN_H_
#define GRAPHFLOW_STORAGE_COLUMN_H_

#include <bits/unique_ptr.h>

#include "src/common/include/types.h"

using namespace graphflow::common;

namespace graphflow {
namespace storage {

template<typename T>
class Column {

    const static uint32_t MAX_NUM_ELEMENTS_IN_SLOT = INT16_MAX /*2 << 16 -1*/;
    gfLabel_t nodeLabel;
    T **slots;

public:
    Column(const gfLabel_t nodeLabel, const uint64_t numElements, T nullElement)
        : nodeLabel(nodeLabel) {
        auto numSlots = getSlotIdx(numElements) + 1;
        auto numElementsInLastSlot = getOffsetInSlot(numElements);
        slots = new T *[numSlots];
        for (auto i = 0u; i < numSlots - 1; i++) {
            slots[i] = new T[MAX_NUM_ELEMENTS_IN_SLOT];
            std::fill(slots[i], slots[i] + MAX_NUM_ELEMENTS_IN_SLOT, nullElement);
        }
        slots[numSlots - 1] = new T[numElementsInLastSlot];
        std::fill(slots[numSlots - 1], slots[numSlots - 1] + numElementsInLastSlot, nullElement);
    }

    ~Column() {
        for (auto i = 0u; i < sizeof(slots) / sizeof(nullptr); i++) {
            delete[](slots[i]);
        }
        delete[](slots);
    }

    inline gfLabel_t getNodeLabel() { return nodeLabel; }

    inline T getVal(gfNodeOffset_t offset) {
        return slots[getSlotIdx(offset)][getOffsetInSlot(offset)];
    }

    inline void setVal(gfNodeOffset_t offset, T val) {
        slots[getSlotIdx(offset)][getOffsetInSlot(offset)] = val;
    }

    inline static uint32_t getSlotIdx(gfNodeOffset_t nodeOffset) {
        return nodeOffset / MAX_NUM_ELEMENTS_IN_SLOT;
    }

    inline static uint32_t getOffsetInSlot(gfNodeOffset_t nodeOffset) {
        return nodeOffset % MAX_NUM_ELEMENTS_IN_SLOT;
    }
};

typedef Column<gfInt_t> ColumnInteger;
typedef Column<gfDouble_t> ColumnDouble;
typedef Column<gfBool_t> ColumnBoolean;
typedef Column<gfString_t> ColumnString;

} // namespace storage
} // namespace graphflow

#endif // GRAPHFLOW_STORAGE_COLUMN_H_
