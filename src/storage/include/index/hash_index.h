#pragma once

#include <bitset>
#include <climits>

#include "src/common/include/configs.h"
#include "src/common/include/file_utils.h"
#include "src/common/include/memory_manager.h"
#include "src/common/include/types.h"
#include "src/common/include/vector/value_vector.h"
#include "src/storage/include/buffer_manager.h"

using namespace std;
using namespace graphflow::common;

namespace graphflow {
namespace storage {

/**
 * Each index is stored in a single file that has 3 components in it.
 *
 * 1. HashIndexHeader is stored in the first page (pageId 0) of the file. It contains the current
 * state of the hash tables along with certain metrics like the number of pages used etc. Remaining
 * bytes of the pageId 0 are left unused as of now.
 *
 * 2. Primary pages are set of pages that contains all primary slots in the hashIndex. A queried
 * key is mapped to one of the slots based on its hash value and is either stored or looked up
 * just in that slot and any chained overflow slots chained to the primary slot.
 *
 * Slot:
 * Each slot consists of a slot header and several entries. The max number of entries in slot is
 * given by HashIndexConfig::SLOT_CAPACITY. The size of the slot is given by (sizeof(SlotHeader) +
 * (SLOT_CAPACITY * sizeof(Entry)).
 *
 * Entry: [key (fixed sized part), node_offset]
 *
 * 3. Overflow pages are set of pages that holds overflow slots. Overflow slots are used to store
 * entries that comes to the designated primary slot that has already been filled to the capacity.
 * Several overflow slots can be chained after the single primary slot as a singly linked link-list.
 * Each slot's SlotHeader has information about the next overflow slot in the chain and also the
 * number of filled entries in that slot.
 *
 * Layout of the file:
 *
 *       -----------------------
 *  0    | PAGE 0 (HEADER)     |
 *       -----------------------
 *  1    | PAGE 0 (HEADER)     |  <-|
 *       -----------------------    |
 *  2    | PAGE 0 (HEADER)     |    |
 *       -----------------------    |  PRIMARY PAGES
 *               ...                |
 *       -----------------------    |
 *  n    | PAGE 0 (HEADER)     |  <-|
 *       -----------------------
 *  n+1  | PAGE 0 (HEADER)     |  <-|
 *       -----------------------    |
 *  n+2  | PAGE 0 (HEADER)     |    |
 *       -----------------------    |  OVERFLOW PAGES
 *               ...                |
 *       -----------------------    |
 *  n+m  | PAGE 0 (HEADER)     |  <-|
 *       -----------------------
 *
 *  Information about the number of primary pages, primary slots, overflow pages and overflow
 *  slots are stored in the HashIndexHeader.
 *
 *  Mode Of Operations:
 *
 *  WRITE MODE: The hashIndex is initially opened in the write mode in which only the insertions
 *  are supported. In here, all the page allocations are happen in memory which needs to be saved
 *  to disk to persist. Also for performance reasons and based on our use case, we support
 *  `bulkReserve` operation to fix the hashIndex capacity in the beginning before any insertions
 *  are made. This is done by means of calculating the number of primary slots and pages are needed
 *  and pre-allocating them.  In lieu of this, we do not support changing the capacity dynamically
 *  by rehashing and splitting of slots. With this scenario, the hashIndex can still insert entries
 *  more than its capacity but these entries will land in chained overflow slots leading to
 *  degraded performance in insertions as well as in look ups.
 *
 *  READ MODE: The hashIndex can be opened in the read mode by supplying it with the name of already
 *  existing file that was previously flushed in the write mode. Lookups happen by reading
 *  arbitrary number of required pages to reach the required slot and iterating it to find the
 *  required value. In this mode, all pages are not kept in memory but rather are made accessible
 *  by our Buffer manager.
 *
 *  */

class HashIndex;

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
    friend class HashIndex;

private:
    HashIndexHeader() = default;

    explicit HashIndexHeader(DataType keyDataType);

    inline void incrementLevel();

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

class HashIndex {

public:
    explicit HashIndex(DataType keyDataType);

    HashIndex(const string& fName, BufferManager& bufferManager, bool isInMemory);

public:
    // Reserves space for at least the specified number of elements.
    void bulkReserve(uint32_t numEntries);

    bool insert(uint8_t* key, node_offset_t value);

    bool lookup(uint8_t* key, node_offset_t& result, BufferManagerMetrics& metrics);

    void saveToDisk(const string& fName);

private:
    bool notExistsInSlot(uint8_t* slot, uint8_t* key);
    void putNewEntryInSlotAndUpdateHeader(uint8_t* slot, uint8_t* key, node_offset_t value);
    uint64_t reserveOvfSlot();

    bool lookupInSlot(const uint8_t* slot, uint8_t* key, node_offset_t& result) const;

    inline uint64_t calculateSlotIdForHash(hash_t hash) const;

    inline uint64_t getPageIdForSlot(uint64_t slotId) const {
        return slotId / indexHeader.numSlotsPerPage;
    }

    inline uint64_t getSlotIdInPageForSlot(uint64_t slotId) const {
        return slotId % indexHeader.numSlotsPerPage;
    }

    uint8_t* getSlotFromPrimaryPages(uint64_t slotId) const;

    uint8_t* getOvfSlotFromOvfPages(uint64_t slotId) const;

    const uint8_t* getSlotInAPage(const uint8_t* page, uint32_t slotIdInPage) const {
        return page + (slotIdInPage * indexHeader.numBytesPerSlot);
    }

    inline uint8_t* getEntryInSlot(uint8_t* slot, uint32_t entryId) const {
        return slot + sizeof(SlotHeader) + (entryId * indexHeader.numBytesPerEntry);
    }

    static uint8_t* getNewPage();

    hash_t getHashOfKey(uint8_t* key);

private:
    HashIndexHeader indexHeader;
    shared_ptr<spdlog::logger> logger;

    // used only when the hash index is instantiated in the write mode
    vector<unique_ptr<uint8_t[]>> primaryPages{};
    vector<unique_ptr<uint8_t[]>> ovfPages{};

    // used only when the hash index is instantiated in the read mode.
    unique_ptr<FileHandle> fileHandle;
    BufferManager* bufferManager{};
};

} // namespace storage
} // namespace graphflow
