#pragma once

#include "src/storage/index/include/hash_index_utils.h"
#include "src/storage/storage_structure/include/in_mem_file.h"

using namespace graphflow::storage;

namespace graphflow {
namespace storage {

using entry_pos_t = uint8_t;
using slot_id_t = uint64_t;

struct SlotInfo {
    slot_id_t slotId;
    bool isPSlot;
};

class SlotHeader {
public:
    static const entry_pos_t INVALID_ENTRY_POS = UINT8_MAX;

    SlotHeader() : numEntries{0}, validityMask{0}, nextOvfSlotId{0} {}

    void reset() {
        numEntries = 0;
        validityMask = 0;
        nextOvfSlotId = 0;
    }

    inline bool isEntryValid(uint32_t entryPos) const {
        return validityMask & ((uint32_t)1 << entryPos);
    }
    inline void setEntryInvalid(entry_pos_t entryPos) {
        validityMask &= ~((uint32_t)1 << entryPos);
    }
    inline void setEntryValid(entry_pos_t entryPos) { validityMask |= ((uint32_t)1 << entryPos); }

public:
    entry_pos_t numEntries;
    uint32_t validityMask;
    slot_id_t nextOvfSlotId;
};

class HashIndexHeader {

public:
    explicit HashIndexHeader(DataTypeID keyDataType);
    HashIndexHeader(const HashIndexHeader& other) = default;

    void incrementNextSplitSlotId();
    inline void incrementLevel() {
        currentLevel++;
        nextSplitSlotId = 0;
        levelHashMask = (1 << currentLevel) - 1;
        higherLevelHashMask = (1 << (currentLevel + 1)) - 1;
    }

public:
    uint32_t numBytesPerEntry{0};
    uint32_t numBytesPerSlot{0};
    uint32_t numSlotsPerPage{0};
    uint64_t currentLevel{1};
    uint64_t levelHashMask{1};
    uint64_t higherLevelHashMask{3};
    slot_id_t nextSplitSlotId{0};
    uint64_t pPagesMapperFirstPageIdx{0};
    uint64_t oPagesMapperFirstPageIdx{0};
    DataTypeID keyDataTypeID;
};

class SlotArray {

public:
    SlotArray(InMemFile* file, slot_id_t numSlotsPerPage)
        : file{file}, numSlotsPerPage{numSlotsPerPage}, nextSlotIdToAllocate{0} {}

    virtual ~SlotArray() {}

    virtual void addNewPagesWithoutLock(uint64_t numPages);
    inline virtual slot_id_t allocateSlot() {
        unique_lock xLck{sharedGlobalMutex};
        return allocateSlotWithoutLock();
    }
    slot_id_t allocateSlotWithoutLock();
    virtual PageElementCursor getPageCursorForSlot(slot_id_t slotId) {
        shared_lock sLck{sharedGlobalMutex};
        return getPageCursorForSlotWithoutLock(slotId);
    }

    inline void setNextSlotIdToAllocate(slot_id_t slotId) { nextSlotIdToAllocate = slotId; }
    inline slot_id_t getNumAllocatedSlots() const { return nextSlotIdToAllocate; }

public:
    shared_mutex sharedGlobalMutex;
    vector<page_idx_t> pagesLogicalToPhysicalMapper;

protected:
    PageElementCursor getPageCursorForSlotWithoutLock(slot_id_t slotId);

    InMemFile* file;
    const slot_id_t numSlotsPerPage;
    slot_id_t nextSlotIdToAllocate;
};

class SlotWithMutexArray : public SlotArray {

public:
    SlotWithMutexArray(InMemFile* file, slot_id_t numSlotsPerPage)
        : SlotArray{file, numSlotsPerPage} {}

    inline void addNewPagesWithoutLock(uint64_t numPages) override {
        SlotArray::addNewPagesWithoutLock(numPages);
        addSlotMutexesForPagesWithoutLock(numPages);
    }
    void addSlotMutexesForPagesWithoutLock(uint64_t numPages);
    inline slot_id_t allocateSlot() override { return allocateSlotWithoutLock(); }
    inline PageElementCursor getPageCursorForSlot(slot_id_t slotId) override {
        return getPageCursorForSlotWithoutLock(slotId);
    }

    inline void lockSlot(slot_id_t slotId) {
        shared_lock sLck{sharedGlobalMutex};
        mutexes[slotId]->lock();
    }
    inline void unlockSlot(slot_id_t slotId) {
        shared_lock sLck{sharedGlobalMutex};
        mutexes[slotId]->unlock();
    }

private:
    vector<unique_ptr<mutex>> mutexes;
};

/**
 * Basic index file consists of four main components.
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
 * SlotHeader: [numEntries, validityMask, nextOvfSlotId]
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
    explicit BaseHashIndex(const DataType& keyDataType) : numEntries{0} {
        keyHashFunc = HashIndexUtils::initializeHashFunc(keyDataType.typeID);
    }

    virtual ~BaseHashIndex() {}

protected:
    slot_id_t getPrimarySlotIdForKey(const uint8_t* key);
    inline void lockSlot(SlotInfo& slotInfo) {
        assert(slotInfo.isPSlot);
        pSlots->lockSlot(slotInfo.slotId);
    }
    inline void unlockSlot(const SlotInfo& slotInfo) {
        assert(slotInfo.isPSlot);
        pSlots->unlockSlot(slotInfo.slotId);
    }
    inline uint8_t* getEntryInSlot(uint8_t* slot, entry_pos_t entryPos) const {
        return slot + sizeof(SlotHeader) + (entryPos * indexHeader->numBytesPerEntry);
    }

protected:
    unique_ptr<HashIndexHeader> indexHeader;
    hash_function_t keyHashFunc;
    unique_ptr<SlotWithMutexArray> pSlots;
    unique_ptr<SlotArray> oSlots;
    atomic<uint64_t> numEntries;
};

class InMemHashIndex : public BaseHashIndex {
    friend class HashIndexLocalStorage;

public:
    InMemHashIndex(string fName, const DataType& keyDataType);

public:
    // Reserves space for at least the specified number of elements.
    void bulkReserve(uint32_t numEntries);

    // Note: append assumes that bulkRserve has been called before it and the index has reserved
    // enough space already.
    inline bool append(int64_t key, node_offset_t value) {
        return appendInternal(reinterpret_cast<const uint8_t*>(&key), value);
    }
    // TODO(Guodong): change const char* to gfString
    inline bool append(const char* key, node_offset_t value) {
        return appendInternal(reinterpret_cast<const uint8_t*>(key), value);
    }
    inline bool lookup(int64_t key, node_offset_t& result) {
        return lookupInternalWithoutLock(reinterpret_cast<const uint8_t*>(&key), result);
    }

    // Non-thread safe. This should only be called in the loader and never be called in parallel.
    void flush();

private:
    bool appendInternal(const uint8_t* key, node_offset_t value);
    bool deleteInternal(const uint8_t* key);
    bool lookupInternalWithoutLock(const uint8_t* key, node_offset_t& result);

    template<bool IS_LOOKUP>
    bool lookupOrExistsInSlotWithoutLock(
        uint8_t* slot, const uint8_t* key, node_offset_t* result = nullptr);
    void insertToSlotWithoutLock(uint8_t* slot, const uint8_t* key, node_offset_t value);
    void splitSlotWithoutLock();
    void rehashSlotsWithoutLock();
    void copyEntryToSlotWithoutLock(slot_id_t slotId, uint8_t* entry);
    vector<uint8_t*> getPAndOSlotsWithoutLock(slot_id_t pSlotId);
    entry_pos_t findMatchedEntryInSlotWithoutLock(uint8_t* slot, const uint8_t* key);

    inline uint8_t* getSlot(const SlotInfo& slotInfo) const {
        auto pageCursor = slotInfo.isPSlot ? pSlots->getPageCursorForSlot(slotInfo.slotId) :
                                             oSlots->getPageCursorForSlot(slotInfo.slotId);
        return inMemFile->getPage(pageCursor.pageIdx)->data +
               (pageCursor.posInPage * indexHeader->numBytesPerSlot);
    }

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
