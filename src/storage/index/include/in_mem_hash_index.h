#pragma once

#include "src/storage/index/include/hash_index_utils.h"
#include "src/storage/storage_structure/include/in_mem_file.h"

using namespace graphflow::storage;

namespace graphflow {
namespace storage {

struct SlotInfo {
    uint64_t slotId;
    bool isPSlot;
};

class SlotHeader {
public:
    SlotHeader() : numEntries{0}, deletionMask{0}, nextOvfSlotId{0} {}

    void reset() {
        numEntries = 0;
        deletionMask = 0;
        nextOvfSlotId = 0;
    }

    inline bool isEntryDeleted(uint32_t entryPos) const {
        return deletionMask & ((uint32_t)1 << entryPos);
    }
    inline void setEntryDeleted(uint32_t entryPos) { deletionMask |= ((uint32_t)1 << entryPos); }

public:
    uint8_t numEntries;
    uint32_t deletionMask;
    uint64_t nextOvfSlotId;
};

class HashIndexHeader {

public:
    explicit HashIndexHeader(DataTypeID keyDataType);
    HashIndexHeader(const HashIndexHeader& other) = default;

    inline void incrementLevel() {
        currentLevel++;
        levelHashMask = (1 << currentLevel) - 1;
        higherLevelHashMask = (1 << (currentLevel + 1)) - 1;
    }

public:
    uint32_t numBytesPerEntry{0};
    uint32_t numBytesPerSlot{0};
    uint32_t numSlotsPerPage{0};
    uint64_t currentLevel{0};
    uint64_t levelHashMask{0};
    uint64_t higherLevelHashMask{0};
    uint64_t nextSplitSlotId{0};
    uint64_t pPagesMapperFirstPageIdx{0};
    uint64_t oPagesMapperFirstPageIdx{0};
    DataTypeID keyDataTypeID;
};

class SlotArray {

public:
    SlotArray(InMemFile* file, uint64_t numSlotsPerPage)
        : file{file}, numSlotsPerPage{numSlotsPerPage}, nextSlotIdToAllocate{0}, slotsCapacity{0} {}

    virtual ~SlotArray() {}

    virtual void addNewPagesWithoutLock(uint64_t numPages);
    uint64_t allocateSlot();
    PageElementCursor getPageCursorForSlot(uint64_t slotId);

    void setNextSlotIdToAllocate(uint64_t slotId) { nextSlotIdToAllocate = slotId; }

public:
    shared_mutex sharedGlobalMutex;
    vector<page_idx_t> pagesLogicalToPhysicalMapper;

protected:
    InMemFile* file;
    const uint64_t numSlotsPerPage;
    uint64_t nextSlotIdToAllocate;
    uint64_t slotsCapacity;
};

class SlotWithMutexArray : public SlotArray {

public:
    SlotWithMutexArray(InMemFile* file, uint64_t numSlotsPerPage)
        : SlotArray{file, numSlotsPerPage} {}

    void addNewPagesWithoutLock(uint64_t numPages) override;
    void addSlotMutexesForPagesWithoutLock(uint64_t numPages);

    inline void lockSlot(uint64_t slotId) {
        shared_lock sLck{sharedGlobalMutex};
        mutexes[slotId]->lock();
    }
    inline void unlockSlot(uint64_t slotId) {
        shared_lock sLck{sharedGlobalMutex};
        mutexes[slotId]->unlock();
    }

private:
    vector<unique_ptr<mutex>> mutexes;
};

/**
 * Each index is stored in a single file that has 4 components in it.
 *
 * 1. HashIndexHeader is stored in the first page (pageId 0) of the file. It contains the current
 * state of the hash tables (level and split information: currentLevel, levelHashMask,
 * higherLevelHashMask, nextSplitSlotId; pPagesMapperFirstPageIdx; oPagesMapperFirstPageIdx; key
 * data type). Remaining bytes of the pageId 0 are left unused as of now.
 *
 * 2. Primary pages are set of pages that contains all primary slots (pSlots) in the hashIndex. A
 * key is mapped to one of the pSlots based on its hash value and the level and splitting info. The
 * actual key and value are either stored in the pSlot, or in a chained overflow slots (oSlots) of
 * the pSlot. Each pSlot corresponds to an mutex, whose unique lock is obtained before any
 * insertions or deletions to that pSlot or its chained oSlots.
 *
 * The slot data structure:
 * Each slot (p/oSlot) consists of a slot header and several entries. The max number of entries in
 * slot is given by HashIndexConfig::SLOT_CAPACITY. The size of the slot is given by
 * (sizeof(SlotHeader) + (SLOT_CAPACITY * sizeof(Entry)).
 *
 * SlotHeader: [numEntries, deletionMask, nextOvfSlotId]
 * Entry: [key (fixed sized part), node_offset]
 *
 * 3. Overflow pages are set of pages that holds overflow slots (oSlots). oSlots are used to
 * store entries that comes to the designated primary slot that has already been filled to the
 * capacity. Several overflow slots can be chained after the single primary slot as a singly linked
 * link-list. Each slot's SlotHeader has information about the next overflow slot in the chain and
 * also the number of filled entries in that slot.
 *
 * Physical layout of the file after constructing from scratch:
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
 * 4. MAPPING PAGES contains the mapping of logical primary/ovf pages to the actual physical pageIdx
 * in the index file. The mapping is written when flushing the index, and read when initializing the
 * index.
 *
 *  */

class BaseHashIndex {
public:
    explicit BaseHashIndex(const DataType& keyDataType) {
        keyHashFunc = HashIndexUtils::initializeHashFunc(keyDataType.typeID);
    }

    virtual ~BaseHashIndex() {}

    virtual void flush() = 0;

protected:
    inline uint64_t getPrimarySlotIdForKey(const uint8_t* key) {
        auto hash = keyHashFunc(key);
        auto slotId = hash & indexHeader->levelHashMask;
        if (slotId < indexHeader->nextSplitSlotId) {
            slotId = hash & indexHeader->higherLevelHashMask;
        }
        return slotId;
    }
    inline void lockSlot(SlotInfo& slotInfo) {
        assert(slotInfo.isPSlot);
        pSlots->lockSlot(slotInfo.slotId);
    }
    inline void unlockSlot(const SlotInfo& slotInfo) {
        assert(slotInfo.isPSlot);
        pSlots->unlockSlot(slotInfo.slotId);
    }
    inline uint8_t* getEntryInSlot(uint8_t* slot, uint32_t entryId) const {
        return slot + sizeof(SlotHeader) + (entryId * indexHeader->numBytesPerEntry);
    }

protected:
    unique_ptr<HashIndexHeader> indexHeader;
    hash_function_t keyHashFunc;
    unique_ptr<SlotWithMutexArray> pSlots;
    unique_ptr<SlotArray> oSlots;
};

class InMemHashIndexBuilder : public BaseHashIndex {
public:
    InMemHashIndexBuilder(string fName, const DataType& keyDataType);

public:
    // Reserves space for at least the specified number of elements.
    // Note: This function is NOT thread-safe.
    void bulkReserve(uint32_t numEntries);

    inline bool append(int64_t key, node_offset_t value) {
        return appendInternal(reinterpret_cast<uint8_t*>(&key), value);
    }
    inline bool append(const char* key, node_offset_t value) {
        return appendInternal(reinterpret_cast<const uint8_t*>(key), value);
    }

    void flush() override;

private:
    bool appendInternal(const uint8_t* key, node_offset_t value);
    bool existsInSlot(uint8_t* slot, const uint8_t* key) const;
    void insertToSlot(uint8_t* slot, const uint8_t* key, node_offset_t value);

    inline uint8_t* getSlot(const SlotInfo& slotInfo) const {
        auto pageCursor = slotInfo.isPSlot ? pSlots->getPageCursorForSlot(slotInfo.slotId) :
                                             oSlots->getPageCursorForSlot(slotInfo.slotId);
        return inMemFile->getPage(pageCursor.pageIdx)->data +
               (pageCursor.posInPage * indexHeader->numBytesPerSlot);
    }

    // TODO(Guodong): This should be refactored to use InMemDiskArray.
    page_idx_t writeLogicalToPhysicalMapper(SlotArray* slotsArray);

private:
    string fName;
    in_mem_insert_function_t keyInsertFunc;
    in_mem_equals_function_t keyEqualsFunc;
    unique_ptr<InMemFile> inMemFile;
    unique_ptr<InMemOverflowFile> inMemOverflowFile;
};

} // namespace storage
} // namespace graphflow
