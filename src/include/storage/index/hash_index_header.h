#pragma once

#include "common/types/types.h"
#include "hash_index_slot.h"

namespace kuzu {
namespace storage {

class HashIndexHeader {
public:
    explicit HashIndexHeader(common::PhysicalTypeID keyDataTypeID)
        : currentLevel{1}, levelHashMask{1}, higherLevelHashMask{3}, nextSplitSlotId{0},
          numEntries{0}, keyDataTypeID{keyDataTypeID},
          firstFreeOverflowSlotId{SlotHeader::INVALID_OVERFLOW_SLOT_ID} {}

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
    // Id of the next slot to split when resizing the hash index
    slot_id_t nextSplitSlotId;
    uint64_t numEntries;
    common::PhysicalTypeID keyDataTypeID;
    // Id of the first in a chain of empty overflow slots which have been reclaimed during slot
    // splitting. The nextOvfSlotId field in the slot's header indicates the next slot in the chain.
    // These slots should be used first when allocating new overflow slots
    // TODO(bmwinger): Make use of this in the on-disk hash index
    slot_id_t firstFreeOverflowSlotId;
};

} // namespace storage
} // namespace kuzu
