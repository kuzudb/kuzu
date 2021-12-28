#pragma once

#include <bitset>
#include <climits>

#include "src/common/include/configs.h"
#include "src/common/include/file_utils.h"
#include "src/common/include/memory_manager.h"
#include "src/common/include/types.h"
#include "src/common/include/vector/value_vector.h"
#include "src/loader/include/in_mem_structure/in_mem_pages.h"
#include "src/storage/include/buffer_manager.h"
#include "src/storage/include/data_structure/string_overflow_pages.h"

using namespace std;
using namespace graphflow::common;
using namespace graphflow::loader;

namespace graphflow {
namespace storage {

class HashIndexUtils;
class HashIndexReader;
class HashIndexBuilder;

struct SlotHeader {
public:
    SlotHeader() : numEntries{0}, nextOvfSlotId{-1u} {}

    void reset() {
        numEntries = 0;
        nextOvfSlotId = 0;
    }

public:
    uint16_t numEntries;
    uint64_t nextOvfSlotId;
};

struct HashIndexHeader {
    friend class HashIndexUtils;
    friend class HashIndexReader;
    friend class HashIndexBuilder;

private:
    HashIndexHeader() = default;

    explicit HashIndexHeader(DataType keyDataType);

    void incrementLevel();

    // Constants
    uint64_t numBytesPerEntry{};
    uint64_t numBytesPerSlot{};
    uint64_t numSlotsPerPage{};
    uint64_t slotCapacity{HashIndexConfig::SLOT_CAPACITY};

    uint64_t numEntries{0};
    uint64_t numPrimarySlots{2};
    uint64_t numPrimaryPages{1};
    uint64_t numOvfSlots{1};
    uint64_t numOvfPages{1};

    uint64_t currentLevel{1};
    uint64_t levelHashMask{1};
    uint64_t higherLevelHashMask{3};
    uint64_t nextSplitSlotId{0};

    DataType keyDataType = INT64;
};

class HashIndexUtils {

public:
    static uint64_t calculateSlotIdForHash(const HashIndexHeader& header, hash_t hash);

    static inline uint64_t getPageIdForSlot(const HashIndexHeader& header, uint64_t slotId) {
        return slotId / header.numSlotsPerPage;
    }

    static inline uint64_t getSlotIdInPageForSlot(const HashIndexHeader& header, uint64_t slotId) {
        return slotId % header.numSlotsPerPage;
    }

    static const uint8_t* getSlotInAPage(
        const HashIndexHeader& header, const uint8_t* page, uint32_t slotIdInPage) {
        return page + (slotIdInPage * header.numBytesPerSlot);
    }

    static inline uint8_t* getEntryInSlot(
        const HashIndexHeader& header, uint8_t* slot, uint32_t entryId) {
        return slot + sizeof(SlotHeader) + (entryId * header.numBytesPerEntry);
    }

    static uint8_t* getNewPage();

    static hash_t hashFunc(const HashIndexHeader& header, uint8_t* key);

    static bool equalsInt64(uint8_t* key, uint8_t* entryKey);
    static bool likelyEqualsString(uint8_t* key, gf_string_t* entryKey);
};

} // namespace storage
} // namespace graphflow
