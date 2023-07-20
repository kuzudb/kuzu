#pragma once

#include <cstdint>

#include "common/constants.h"
#include "common/types/internal_id_t.h"
#include "common/types/ku_string.h"
#include "storage/index/hash_index_utils.h"

namespace kuzu {
namespace storage {

using entry_pos_t = uint8_t;
using slot_id_t = uint64_t;

class SlotHeader {
public:
    static const entry_pos_t INVALID_ENTRY_POS = UINT8_MAX;

    SlotHeader() : numEntries{0}, validityMask{0}, nextOvfSlotId{0} {}

    void reset() {
        numEntries = 0;
        validityMask = 0;
        partialHash = 0;
        nextOvfSlotId = 0;
    }

    inline bool isEntryValid(uint32_t entryPos) const {
        return validityMask & ((uint32_t)1 << entryPos);
    }
    inline void setEntryValid(entry_pos_t entryPos) { validityMask |= ((uint32_t)1 << entryPos); }
    inline void setEntryInvalid(entry_pos_t entryPos) {
        validityMask &= ~((uint32_t)1 << entryPos);
    }

    inline void setPartialHash(entry_pos_t entryPos, kuzu::common::hash_t hash) {
        uint8_t partial = HashIndexUtils::compute_tag(hash);

        uint8_t shift_bits = (8*entryPos);
        uint32_t bitmask = ~(0xFF << shift_bits);
        uint32_t shifted_replacement = static_cast<uint32_t>(partial) << shift_bits;

        partialHash &= bitmask;
        partialHash |= shifted_replacement;

    }

    inline uint8_t getPartialHash(entry_pos_t entryPos) {

        uint8_t extracted_partial = 0;
        uint8_t shift_bits = (8*entryPos);

        extracted_partial = (partialHash >> shift_bits) & 0xFF;
        return extracted_partial;
    }

    inline bool isPartialHashMatch(entry_pos_t entryPos, kuzu::common::hash_t hash) {
        uint8_t extracted_partial = getPartialHash(entryPos);
        return HashIndexUtils::compute_tag(hash) == extracted_partial;
    }
    
public:
    entry_pos_t numEntries;
    uint32_t partialHash;
    uint32_t validityMask;
    slot_id_t nextOvfSlotId;
};

template<typename T>
struct SlotEntry {
    uint8_t data[sizeof(T) + sizeof(common::offset_t)];
};

template<typename T>
struct Slot {
    SlotHeader header;
    SlotEntry<T> entries[common::HashIndexConstants::SLOT_CAPACITY];
};

} // namespace storage
} // namespace kuzu
