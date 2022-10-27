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

public:
    uint64_t currentLevel{0};
    uint64_t levelHashMask{0};
    uint64_t higherLevelHashMask{1};
    slot_id_t nextSplitSlotId{0};
    common::DataTypeID keyDataTypeID{common::INT64};
};

} // namespace storage
} // namespace graphflow
