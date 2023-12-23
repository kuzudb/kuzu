#pragma once

#include "common/mutex.h"
#include "hash_index_header.h"
#include "hash_index_slot.h"
#include "storage/index/hash_index_utils.h"
#include "storage/storage_structure/disk_array.h"
#include "storage/storage_structure/in_mem_file.h"

namespace kuzu {
namespace storage {

static constexpr common::page_idx_t INDEX_HEADER_ARRAY_HEADER_PAGE_IDX = 0;
static constexpr common::page_idx_t P_SLOTS_HEADER_PAGE_IDX = 1;
static constexpr common::page_idx_t O_SLOTS_HEADER_PAGE_IDX = 2;
static constexpr common::page_idx_t NUM_HEADER_PAGES = 3;
static constexpr uint64_t INDEX_HEADER_IDX_IN_ARRAY = 0;

/**
 * Basic index file consists of three disk arrays: indexHeader, primary slots (pSlots), and overflow
 * slots (oSlots).
 *
 * 1. HashIndexHeader contains the current state of the hash tables (level and split information:
 * currentLevel, levelHashMask, higherLevelHashMask, nextSplitSlotId;  key data type).
 *
 * 2. Given a key, it is mapped to one of the pSlots based on its hash value and the level and
 * splitting info. The actual key and value are either stored in the pSlot, or in a chained overflow
 * slots (oSlots) of the pSlot. Each pSlot corresponds to an mutex, whose unique lock is obtained
 * before any insertions or deletions to that pSlot or its chained oSlots.
 *
 * The slot data structure:
 * Each slot (p/oSlot) consists of a slot header and several entries. The max number of entries in
 * slot is given by HashIndexConstants::SLOT_CAPACITY. The size of the slot is given by
 * (sizeof(SlotHeader) + (SLOT_CAPACITY * sizeof(Entry)).
 *
 * SlotHeader: [numEntries, validityMask, nextOvfSlotId]
 * Entry: [key (fixed sized part), node_offset]
 *
 * 3. oSlots are used to store entries that comes to the designated primary slot that has already
 * been filled to the capacity. Several overflow slots can be chained after the single primary slot
 * as a singly linked link-list. Each slot's SlotHeader has information about the next overflow slot
 * in the chain and also the number of filled entries in that slot.
 *
 *  */

enum class SlotType : uint8_t { PRIMARY = 0, OVF = 1 };

struct SlotInfo {
    slot_id_t slotId{UINT64_MAX};
    SlotType slotType{SlotType::PRIMARY};
};

class BaseHashIndex {
public:
    explicit BaseHashIndex(const common::LogicalType& keyDataType) {
        keyHashFunc = HashIndexUtils::initializeHashFunc(keyDataType.getLogicalTypeID());
    }

    virtual ~BaseHashIndex() = default;

protected:
    slot_id_t getPrimarySlotIdForKey(const HashIndexHeader& indexHeader, const uint8_t* key);

    static inline uint64_t getNumRequiredEntries(
        uint64_t numExistingEntries, uint64_t numNewEntries) {
        return ceil((double)(numExistingEntries + numNewEntries) * common::DEFAULT_HT_LOAD_FACTOR);
    }

protected:
    std::unique_ptr<HashIndexHeader> indexHeader;
    std::shared_mutex pSlotSharedMutex;
    hash_function_t keyHashFunc;
};

template<typename T>
class HashIndexBuilder : public BaseHashIndex {
public:
    HashIndexBuilder(const std::shared_ptr<FileHandle>& handle,
        const std::shared_ptr<common::Mutex<InMemFile>>& overflowFile, uint64_t indexPos,
        const common::LogicalType& keyDataType);

public:
    // Reserves space for at least the specified number of elements.
    void bulkReserve(uint32_t numEntries);

    // Note: append assumes that bulkReserve has been called before it and the index has reserved
    // enough space already.
    bool append(const uint8_t* key, common::offset_t value);
    bool lookup(const uint8_t* key, common::offset_t& result);

    // Non-thread safe. This should only be called in the copyCSV and never be called in parallel.
    void flush();

private:
    template<bool IS_LOOKUP>
    bool lookupOrExistsInSlotWithoutLock(
        Slot<T>* slot, const uint8_t* key, common::offset_t* result = nullptr);
    void insertToSlotWithoutLock(Slot<T>* slot, const uint8_t* key, common::offset_t value);
    Slot<T>* getSlot(const SlotInfo& slotInfo);
    uint32_t allocatePSlots(uint32_t numSlotsToAllocate);
    uint32_t allocateAOSlot();

private:
    std::shared_ptr<FileHandle> fileHandle;
    std::shared_ptr<common::Mutex<InMemFile>> inMemOverflowFile;
    std::unique_ptr<InMemDiskArrayBuilder<HashIndexHeader>> headerArray;
    std::shared_mutex oSlotsSharedMutex;
    std::unique_ptr<InMemDiskArrayBuilder<Slot<T>>> pSlots;
    std::unique_ptr<InMemDiskArrayBuilder<Slot<T>>> oSlots;
    std::vector<std::unique_ptr<std::mutex>> pSlotsMutexes;
    in_mem_insert_function_t keyInsertFunc;
    in_mem_equals_function_t keyEqualsFunc;
    uint8_t slotCapacity;
    std::atomic<uint64_t> numEntries;
};

class PrimaryKeyIndexBuilder {
public:
    PrimaryKeyIndexBuilder(const std::string& fName, const common::LogicalType& keyDataType,
        common::VirtualFileSystem* vfs);

    void bulkReserve(uint32_t numEntries);

    // Note: append assumes that bulkRserve has been called before it and the index has reserved
    // enough space already.
    bool append(int64_t key, common::offset_t value) {
        return appendWithIndexPos(key, value, getHashIndexPosition(key));
    }
    bool append(const char* key, common::offset_t value) {
        return appendWithIndexPos(key, value, getHashIndexPosition(key));
    }
    bool appendWithIndexPos(int64_t key, common::offset_t value, uint64_t indexPos) {
        KU_ASSERT(keyDataTypeID == common::LogicalTypeID::INT64);
        KU_ASSERT(getHashIndexPosition(key) == indexPos);
        return hashIndexBuilderForInt64[indexPos]->append(
            reinterpret_cast<const uint8_t*>(&key), value);
    }
    bool appendWithIndexPos(const char* key, common::offset_t value, uint64_t indexPos) {
        KU_ASSERT(keyDataTypeID == common::LogicalTypeID::STRING);
        KU_ASSERT(getHashIndexPosition(key) == indexPos);
        return hashIndexBuilderForString[indexPos]->append(
            reinterpret_cast<const uint8_t*>(key), value);
    }
    bool lookup(int64_t key, common::offset_t& result) {
        KU_ASSERT(keyDataTypeID == common::LogicalTypeID::INT64);
        return hashIndexBuilderForInt64[getHashIndexPosition(key)]->lookup(
            reinterpret_cast<const uint8_t*>(&key), result);
    }
    bool lookup(const char* key, common::offset_t& result) {
        KU_ASSERT(keyDataTypeID == common::LogicalTypeID::STRING);
        return hashIndexBuilderForString[getHashIndexPosition(key)]->lookup(
            reinterpret_cast<const uint8_t*>(key), result);
    }

    // Not thread safe.
    void flush();

    common::LogicalTypeID keyTypeID() const { return keyDataTypeID; }

private:
    std::mutex mtx;
    common::LogicalTypeID keyDataTypeID;
    std::shared_ptr<common::Mutex<InMemFile>> overflowFile;
    std::vector<std::unique_ptr<HashIndexBuilder<int64_t>>> hashIndexBuilderForInt64;
    std::vector<std::unique_ptr<HashIndexBuilder<common::ku_string_t>>> hashIndexBuilderForString;
};

} // namespace storage
} // namespace kuzu
