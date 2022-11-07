#pragma once

#include "hash_index_slot.h"

#include "src/common/types/include/types.h"

namespace graphflow {
namespace storage {

class HashIndexHeader {
public:
    inline void incrementLevel() {
        currentLevel++;
        nextSplitSlotId = 0;
        levelHashMask = (1 << currentLevel) - 1;
        higherLevelHashMask = (1 << (currentLevel + 1)) - 1;
    }
    inline void incrementNextSplitSlotId() {
        if (nextSplitSlotId < (1ull << currentLevel) - 1) {
            nextSplitSlotId++;
        } else {
            incrementLevel();
        }
    }

public:
    uint64_t currentLevel{1};
    uint64_t levelHashMask{1};
    uint64_t higherLevelHashMask{3};
    slot_id_t nextSplitSlotId{0};
    uint64_t numEntries{0};
    uint32_t numBytesPerEntry{sizeof(int64_t) + sizeof(common::node_offset_t)};
    common::DataTypeID keyDataTypeID{common::INT64};
};

} // namespace storage
} // namespace graphflow
