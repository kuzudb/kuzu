#pragma once

#include "src/loader/in_mem_index/include/in_mem_hash_index_utils.h"
#include "src/loader/in_mem_storage_structure/include/in_mem_file.h"
#include "src/storage/index/include/hash_index_utils.h"

using namespace graphflow::storage;

namespace graphflow {
namespace loader {

// TODO(Guodong): check and refactor duplicated code with HashIndex.
class InMemHashIndexBuilder {
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

    void flush();

private:
    bool appendInternal(const uint8_t* key, node_offset_t value);
    bool existsInSlot(uint8_t* slot, const uint8_t* key) const;
    void insertToSlot(uint8_t* slot, const uint8_t* key, node_offset_t value);
    uint64_t reserveSlot();

    inline uint64_t calculateSlotIdForHash(hash_t hash) {
        auto slotId = hash & indexHeader->levelHashMask;
        slotId = slotId >= indexHeader->nextSplitSlotId ? slotId :
                                                          (hash & indexHeader->higherLevelHashMask);
        return slotId;
    }
    inline void lockSlot(uint64_t slotId) {
        shared_lock lck{sharedLockForSlotMutexes};
        assert(slotId < slotMutexes.size());
        slotMutexes[slotId]->lock();
    }
    inline void unlockSlot(uint64_t slotId) {
        shared_lock lck{sharedLockForSlotMutexes};
        assert(slotId < slotMutexes.size());
        slotMutexes[slotId]->unlock();
    }
    inline uint8_t* getEntryInSlot(uint8_t* slot, uint32_t entryId) const {
        return slot + sizeof(SlotHeader) + (entryId * indexHeader->numBytesPerEntry);
    }
    // When we build the index from scratch, we always call bulkReserve before appending keys. In
    // this way, primary slots are always firstly allocated and never resized, and overflow slots
    // are allocated on the run after primary ones. Thus, when we calculate the page index for a
    // slot, we just need to skip the header page, which is by default the 0th page.
    inline uint64_t getPhysicalPageIdForSlot(uint64_t slotId) const {
        return (slotId / indexHeader->numSlotsPerPage) + 1;
    }
    inline uint64_t getSlotIdInPageForSlot(uint64_t slotId) const {
        return slotId % indexHeader->numSlotsPerPage;
    }
    inline uint8_t* getSlot(uint64_t slotId) const {
        auto pageIdx = getPhysicalPageIdForSlot(slotId);
        auto slotIdxInPage = getSlotIdInPageForSlot(slotId);
        return inMemFile->getPage(pageIdx)->data + (slotIdxInPage * indexHeader->numBytesPerSlot);
    }

    string fName;
    unique_ptr<HashIndexHeader> indexHeader;

    hash_function_t keyHashFunc;
    in_mem_insert_function_t keyInsertFunc;
    in_mem_equals_function_t keyEqualsFunc;

    unique_ptr<InMemFile> inMemFile;
    unique_ptr<InMemOverflowFile> inMemOverflowFile;

    shared_mutex sharedLockForSlotMutexes;
    vector<unique_ptr<mutex>> slotMutexes;
    mutex lockForReservingOvfSlot;
    atomic<uint64_t> numEntries{0};
    uint64_t numSlots{0};
    uint64_t slotsCapacity{0};
};
} // namespace loader
} // namespace graphflow
