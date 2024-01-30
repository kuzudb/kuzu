#pragma once

#include "common/mutex.h"
#include "common/type_utils.h"
#include "common/types/types.h"
#include "storage/index/base_hash_index.h"
#include "storage/index/hash_index_utils.h"
#include "storage/storage_structure/disk_array.h"
#include "storage/storage_structure/in_mem_file.h"
#include "transaction/transaction.h"

namespace kuzu {
namespace storage {

class InMemHashIndex {
public:
    virtual ~InMemHashIndex() = default;
    virtual void flush() = 0;
    virtual void bulkReserve(uint32_t numEntries) = 0;
};

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

template<common::IndexHashable T, typename S = T>
class HashIndexBuilder final : public InMemHashIndex, BaseHashIndex<T, S> {
public:
    HashIndexBuilder(const std::shared_ptr<FileHandle>& handle,
        const std::shared_ptr<common::Mutex<InMemFile>>& overflowFile, uint64_t indexPos,
        common::PhysicalTypeID keyDataType);

public:
    // Reserves space for at least the specified number of elements.
    void bulkReserve(uint32_t numEntries) override;

    // Note: append assumes that bulkReserve has been called before it and the index has reserved
    // enough space already.
    bool append(T key, common::offset_t value);
    bool lookup(T key, common::offset_t& result);

    // Non-thread safe. This should only be called in the copyCSV and never be called in parallel.
    void flush() override;

private:
    template<bool IS_LOOKUP>
    bool lookupOrExistsInSlotWithoutLock(Slot<S>* slot, T key, common::offset_t* result = nullptr);
    void insertToSlotWithoutLock(Slot<S>* slot, T key, common::offset_t value);
    Slot<S>* getSlot(const SlotInfo& slotInfo);

    inline Slot<S> getSlot(
        transaction::TransactionType /*trxType*/, const SlotInfo& slotInfo) override {
        return *getSlot(slotInfo);
    }
    void updateSlot(const SlotInfo& slotInfo, const Slot<S>& slot) override {
        *getSlot(slotInfo) = slot;
    }
    uint32_t allocatePSlots(uint32_t numSlotsToAllocate);
    inline uint32_t appendPSlot() override { return allocatePSlots(1); }
    uint32_t allocateAOSlot();
    uint64_t appendOverflowSlot(Slot<S>&& slot) override {
        auto index = allocateAOSlot();
        updateSlot(SlotInfo{index, SlotType::OVF}, slot);
        return index;
    }

    inline bool equals(T keyToLookup, const S& keyInEntry, const InMemFile* /*inMemOverflowFile*/) {
        return keyToLookup == keyInEntry;
    }

    // Overridden so that we can specialize for strings
    inline void insert(T key, uint8_t* entry, common::offset_t offset) override {
        return BaseHashIndex<T, S>::insert(key, entry, offset);
    }
    common::hash_t hashStored(
        transaction::TransactionType /*trxType*/, const S& key) const override;
    void insertNoGuard(
        T key, uint8_t* entry, common::offset_t offset, InMemFile* /*inMemOverflowFile*/) {
        insert(key, entry, offset);
    }

private:
    std::shared_ptr<FileHandle> fileHandle;
    std::shared_ptr<common::Mutex<InMemFile>> inMemOverflowFile;
    std::unique_ptr<InMemDiskArrayBuilder<HashIndexHeader>> headerArray;
    std::shared_mutex oSlotsSharedMutex;
    std::unique_ptr<InMemDiskArrayBuilder<Slot<S>>> pSlots;
    std::unique_ptr<InMemDiskArrayBuilder<Slot<S>>> oSlots;
    std::vector<std::unique_ptr<std::mutex>> pSlotsMutexes;
    uint8_t slotCapacity;
    std::atomic<uint64_t> numEntries;
};

class PrimaryKeyIndexBuilder {
public:
    PrimaryKeyIndexBuilder(const std::string& fName, common::PhysicalTypeID keyDataType,
        common::VirtualFileSystem* vfs);

    void bulkReserve(uint32_t numEntries);

    // Note: append assumes that bulkRserve has been called before it and the index has reserved
    // enough space already.
    template<typename T>
    requires common::HashablePrimitive<T> bool append(T key, common::offset_t value) {
        return appendWithIndexPos(key, value, getHashIndexPosition(key));
    }
    bool append(std::string_view key, common::offset_t value) {
        return appendWithIndexPos(key, value, getHashIndexPosition(key));
    }
    template<typename T>
    requires common::HashablePrimitive<T> bool appendWithIndexPos(
        T key, common::offset_t value, uint64_t indexPos) {
        KU_ASSERT(keyDataTypeID == common::TypeUtils::getPhysicalTypeIDForType<T>());
        KU_ASSERT(getHashIndexPosition(key) == indexPos);
        return getTypedHashIndex<T>(indexPos)->append(key, value);
    }
    bool appendWithIndexPos(std::string_view key, common::offset_t value, uint64_t indexPos) {
        KU_ASSERT(keyDataTypeID == common::PhysicalTypeID::STRING);
        KU_ASSERT(getHashIndexPosition(key) == indexPos);
        return getTypedHashIndex<std::string_view, common::ku_string_t>(indexPos)->append(
            key, value);
    }
    template<typename T>
    requires common::HashablePrimitive<T> bool lookup(T key, common::offset_t& result) {
        KU_ASSERT(keyDataTypeID == common::TypeUtils::getPhysicalTypeIDForType<T>());
        return getTypedHashIndex<T>(getHashIndexPosition(key))->lookup(key, result);
    }
    bool lookup(std::string_view key, common::offset_t& result) {
        KU_ASSERT(keyDataTypeID == common::PhysicalTypeID::STRING);
        return getTypedHashIndex<std::string_view, common::ku_string_t>(getHashIndexPosition(key))
            ->lookup(key, result);
    }

    // Not thread safe.
    void flush();

    common::PhysicalTypeID keyTypeID() const { return keyDataTypeID; }

private:
    template<typename T, typename S = T>
    inline HashIndexBuilder<T, S>* getTypedHashIndex(uint64_t indexPos) {
        return common::ku_dynamic_cast<InMemHashIndex*, HashIndexBuilder<T, S>*>(
            hashIndexBuilders[indexPos].get());
    }

private:
    std::mutex mtx;
    common::PhysicalTypeID keyDataTypeID;
    std::shared_ptr<common::Mutex<InMemFile>> overflowFile;
    std::vector<std::unique_ptr<InMemHashIndex>> hashIndexBuilders;
};

} // namespace storage
} // namespace kuzu
