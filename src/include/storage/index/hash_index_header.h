#pragma once

#include "common/types/types.h"
#include "hash_index_slot.h"

namespace kuzu {
namespace storage {

class HashIndexHeader {
public:
    explicit HashIndexHeader(common::PhysicalTypeID keyDataTypeID)
        : currentLevel{1}, levelHashMask{1}, higherLevelHashMask{3}, nextSplitSlotId{0},
          numEntries{0}, keyDataTypeID{keyDataTypeID} {}

    // Used for element initialization in disk array only.
    HashIndexHeader() : HashIndexHeader(common::PhysicalTypeID::STRING) {}

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
    uint64_t currentLevel;
    uint64_t levelHashMask;
    uint64_t higherLevelHashMask;
    slot_id_t nextSplitSlotId;
    uint64_t numEntries;
    common::PhysicalTypeID keyDataTypeID;
};

} // namespace storage
} // namespace kuzu
