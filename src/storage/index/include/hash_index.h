#pragma once

#include <bitset>
#include <climits>

#include "hash_index_utils.h"

#include "src/common/include/configs.h"
#include "src/common/include/file_utils.h"
#include "src/common/include/vector/value_vector.h"
#include "src/function/hash/operations/include/hash_operations.h"
#include "src/loader/include/in_mem_structure/in_mem_file.h"
#include "src/storage/buffer_manager/include/buffer_manager.h"
#include "src/storage/buffer_manager/include/memory_manager.h"
#include "src/storage/storage_structure/include/overflow_pages.h"

using namespace graphflow::common;
using namespace graphflow::loader;

namespace graphflow {
namespace storage {

// TODO(Guodong): Concurrent writes for string overflows; Merge InMemOverflowFile and OverflowPages.
/**
 * Each index is stored in a single file that has 3 components in it.
 *
 * 1. HashIndexHeader is stored in the first page (pageId 0) of the file. It contains the current
 * state of the hash tables (level and split information: currentLevel, levelHashMask,
 * higherLevelHashMask, nextSplitSlotId; key data type) along with certain metrics like numEntries,
 * numPrimaryPages, numOvfPages, etc. Remaining bytes of the pageId 0 are left unused as of now.
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
 * Physical layout of the file:
 *
 *       ---------------------
 *  0    | PAGE 0 (HEADER)   |
 *       ---------------------
 *  1    |       (SLOT)      |  <-|
 *       ---------------------    |
 *  2    |       (SLOT)      |    |
 *       ---------------------    |
 *               ...              | PRIMARY/OVF PAGES
 *       ---------------------    |
 *  k    |       (SLOT)      |  <-|
 *       ---------------------
 *  k+1  |       (SLOT)      |  <-|
 *       ---------------------    |
 *               ...              | MAPPING PAGES
 *       ---------------------  <-|
 *
 * MAPPING PAGES contains the mapping of logical primary/ovf pages to the actual physical pageIdx
 * in the index file. The mapping is written when flushing the index, and read when initializing the
 * index.
 *
 *  Mode Of Operations:
 *
 *  WRITE_ONLY MODE: The hashIndex is initially opened in the write mode in which only the
 * insertions are supported. In here, all the page allocations are happen in memory which needs to
 * be saved to disk to persist. Also for performance reasons and based on our use case, we support
 *  `bulkReserve` operation to fix the hashIndex capacity in the beginning before any insertions
 *  are made. This is done by means of calculating the number of primary slots and pages that are
 * needed and pre-allocating them.  In lieu of this, we do not support changing the capacity
 * dynamically by rehashing and splitting of slots. With this scenario, the hashIndex can still
 * insert entries more than its capacity but these entries will land in chained overflow
 * slots leading to degraded performance in insertions as well as in look ups.
 *
 *  READ_ONLY MODE: The hashIndex can be opened in the read mode by supplying it with the name of
 * already existing file that was previously flushed in the write mode. Lookups happen by reading
 *  arbitrary number of required pages to reach the required slot and iterating it to find the
 *  required value. In this mode, all pages are not kept in memory but rather are made accessible
 *  by our Buffer manager.
 *
 *  */

struct SlotHeader {
public:
    SlotHeader() : numEntries{0}, nextOvfSlotId{0} {}

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

public:
    explicit HashIndexHeader(DataTypeID keyDataType);
    HashIndexHeader(const HashIndexHeader& other) = default;

    inline void incrementLevel();

private:
    uint32_t numBytesPerEntry{0};
    uint32_t numBytesPerSlot{0};
    uint32_t numSlotsPerPage{0};
    uint32_t numPrimaryPages{0};
    uint32_t numOvfPages{0};
    uint64_t numEntries{0};
    uint64_t currentLevel{1};
    uint64_t levelHashMask{1};
    uint64_t higherLevelHashMask{3};
    uint64_t nextSplitSlotId{0};
    DataTypeID keyDataTypeID;
};

struct SlotInfo {
    uint32_t slotId;
    bool isPrimary;
};

constexpr uint64_t INDEX_HEADER_PAGE_ID = 0;

class HashIndex {

    enum IndexMode { READ_ONLY, WRITE };

public:
    HashIndex(string fName, const DataType& keyDataType, BufferManager& bufferManager,
        bool isInMemoryForLookup);

    ~HashIndex();

public:
    // Reserves space for at least the specified number of elements.
    // Note: This function is NOT thread-safe.
    void bulkReserve(uint32_t numEntries);

    inline bool insert(int64_t key, node_offset_t value) {
        return insertInternal(reinterpret_cast<uint8_t*>(&key), value);
    }
    inline bool insert(const char* key, node_offset_t value) {
        return insertInternal(reinterpret_cast<const uint8_t*>(key), value);
    }
    inline bool lookup(int64_t key, node_offset_t& result) {
        return lookupInternal(reinterpret_cast<uint8_t*>(&key), result);
    }
    inline bool lookup(const char* key, node_offset_t& result) {
        return lookupInternal(reinterpret_cast<const uint8_t*>(key), result);
    }

    // Note: This function is NOT thread-safe.
    void flush();

private:
    void initializeHeaderAndPages(const DataType& keyDataType);
    bool insertInternal(const uint8_t* key, node_offset_t value);
    bool lookupInternal(const uint8_t* key, node_offset_t& result);

    bool existsInSlot(uint8_t* slot, const uint8_t* key);
    void insertToSlot(uint8_t* slot, const uint8_t* key, node_offset_t value);
    uint64_t reserveOvfSlot();
    bool lookupInSlot(const uint8_t* slot, const uint8_t* key, node_offset_t& result) const;
    inline uint64_t calculateSlotIdForHash(hash_t hash);
    inline uint64_t getPageIdForSlot(uint64_t slotId) const {
        return slotId / indexHeader->numSlotsPerPage;
    }
    inline uint64_t getSlotIdInPageForSlot(uint64_t slotId) const {
        return slotId % indexHeader->numSlotsPerPage;
    }
    uint8_t* getSlotFromPages(const SlotInfo& slotInfo);
    uint8_t* getSlotInPage(uint8_t* page, uint32_t slotIdInPage) const {
        return page + (slotIdInPage * indexHeader->numBytesPerSlot);
    }
    inline uint8_t* getEntryInSlot(uint8_t* slot, uint32_t entryId) const {
        return slot + sizeof(SlotHeader) + (entryId * indexHeader->numBytesPerEntry);
    }

    void allocateAndCachePageWithoutLock(bool isPrimary);
    inline void setDirtyAndUnPinPage(page_idx_t physicalPageIdx) {
        bm.setPinnedPageDirty(*fh, physicalPageIdx);
        bm.unpin(*fh, physicalPageIdx);
    }

    void lockSlot(const SlotInfo& slotInfo);
    void unLockSlot(const SlotInfo& slotInfo);

    void readLogicalToPhysicalPageMappings();
    void writeLogicalToPhysicalPageMappings();

private:
    string fName;
    bool isInMemoryForLookup;
    bool isFlushed;
    IndexMode indexMode;
    unique_ptr<FileHandle> fh;
    BufferManager& bm;
    unique_ptr<HashIndexHeader> indexHeader;

    hash_function_t keyHashFunc;
    insert_function_t insertKeyToEntryFunc;
    equals_in_write_function_t equalsFuncInWrite;
    equals_in_read_function_t equalsFuncInRead;

    vector<unique_ptr<mutex>> primarySlotMutexes;
    vector<unique_ptr<mutex>> ovfSlotMutexes;
    shared_mutex sharedLockForSlotMutexes;
    vector<page_idx_t> primaryLogicalToPhysicalPagesMapping;
    vector<page_idx_t> ovfLogicalToPhysicalPagesMapping;
    vector<uint8_t*> primaryPinnedFrames;
    vector<uint8_t*> ovfPinnedFrames;

    // Used when writing to the index.
    unique_ptr<InMemOverflowFile> inMemStringOvfPages;
    PageByteCursor stringOvfPageCursor;
    // Used when reading from the index.
    unique_ptr<OverflowPages> stringOvfPages;

    atomic<uint64_t> numEntries{0};
    uint64_t numOvfSlots{1};
    uint64_t ovfSlotsCapacity{0};
    mutex lockForReservingOvfSlot;
};

} // namespace storage
} // namespace graphflow
