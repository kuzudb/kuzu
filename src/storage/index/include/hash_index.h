#pragma once

#include <bitset>
#include <climits>
#include <iostream>

#include "hash_index_utils.h"

#include "src/common/include/configs.h"
#include "src/common/include/file_utils.h"
#include "src/common/include/vector/value_vector.h"
#include "src/function/hash/operations/include/hash_operations.h"
#include "src/storage/buffer_manager/include/buffer_manager.h"
#include "src/storage/buffer_manager/include/memory_manager.h"
#include "src/storage/storage_structure/include/overflow_file.h"

using namespace graphflow::common;

namespace graphflow {
namespace storage {

using slot_id_t = uint64_t;

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

class HashIndex {

    static constexpr bool DELETE_FLAG = true;
    static constexpr bool LOOKUP_FLAG = false;

public:
    HashIndex(
        string fName, const DataType& keyDataType, BufferManager& bufferManager, bool isInMemory);

    ~HashIndex();

public:
    inline bool lookup(int64_t key, node_offset_t& result) {
        return lookupOrDeleteInternal<LOOKUP_FLAG>(reinterpret_cast<uint8_t*>(&key), &result);
    }
    inline bool lookup(const char* key, node_offset_t& result) {
        return lookupOrDeleteInternal<LOOKUP_FLAG>(reinterpret_cast<const uint8_t*>(key), &result);
    }
    inline bool deleteKey(int64_t key) {
        return lookupOrDeleteInternal<DELETE_FLAG>(reinterpret_cast<uint8_t*>(&key));
    }
    inline bool deleteKey(const char* key) {
        return lookupOrDeleteInternal<DELETE_FLAG>(reinterpret_cast<const uint8_t*>(key));
    }

    // Note: This function is NOT thread-safe.
    void flush();

private:
    void initializeFileAndHeader(const DataType& keyDataType);

    uint8_t* pinSlot(slot_id_t slotId);
    void unpinSlot(slot_id_t slotId);

    inline void lockSlot(uint64_t slotId) {
        assert(slotId < slotMutexes.size());
        shared_lock lck{sharedLockForSlotMutexes};
        slotMutexes[slotId]->lock();
    }
    inline void unlockSlot(uint64_t slotId) {
        assert(slotId < slotMutexes.size());
        shared_lock lck{sharedLockForSlotMutexes};
        slotMutexes[slotId]->unlock();
    }

    inline uint64_t calculateSlotIdForHash(hash_t hash) {
        auto slotId = hash & indexHeader->levelHashMask;
        slotId = slotId >= indexHeader->nextSplitSlotId ? slotId :
                                                          (hash & indexHeader->higherLevelHashMask);
        return slotId;
    }
    inline uint64_t getPhysicalPageIdForSlot(uint64_t slotId) const {
        return (slotId / indexHeader->numSlotsPerPage) + 1; // Skip the header page.
    }
    inline uint64_t getSlotIdInPageForSlot(uint64_t slotId) const {
        return slotId % indexHeader->numSlotsPerPage;
    }
    inline uint8_t* getEntryInSlot(uint8_t* slot, uint32_t entryId) const {
        return slot + sizeof(SlotHeader) + (entryId * indexHeader->numBytesPerEntry);
    }

    template<bool IS_DELETE>
    bool lookupOrDeleteInternal(const uint8_t* key, node_offset_t* result = nullptr);
    template<bool IS_DELETE>
    bool lookupOrDeleteInSlot(
        uint8_t* slot, const uint8_t* key, node_offset_t* result = nullptr) const;

private:
    string fName;
    bool isInMemory;
    unique_ptr<FileHandle> fh;
    BufferManager& bm;
    unique_ptr<HashIndexHeader> indexHeader;

    hash_function_t keyHashFunc;
    equals_function_t keyEqualsFunc;

    shared_mutex sharedLockForSlotMutexes;
    vector<unique_ptr<mutex>> slotMutexes;
    mutex lockForReservingOvfSlot;

    unique_ptr<OverflowFile> overflowFile;
    atomic<uint64_t> numEntries{0};
};

} // namespace storage
} // namespace graphflow
