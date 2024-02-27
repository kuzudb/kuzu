#pragma once

#include "common/type_utils.h"
#include "common/types/types.h"
#include "storage/index/hash_index_header.h"
#include "storage/index/hash_index_slot.h"
#include "storage/index/hash_index_utils.h"
#include "storage/storage_structure/disk_array.h"
#include "storage/storage_structure/in_mem_file.h"

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
 * slots (oSlots) of the pSlot.
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

// T is the key type used to access values
// S is the stored type, which is usually the same as T, with the exception of strings
template<common::IndexHashable T, typename S = T>
class HashIndexBuilder final : public InMemHashIndex {
    static_assert(getSlotCapacity<T>() <= SlotHeader::FINGERPRINT_CAPACITY);
    // Size of the validity mask
    static_assert(getSlotCapacity<T>() <= sizeof(SlotHeader().validityMask) * 8);
    static_assert(getSlotCapacity<T>() <= std::numeric_limits<entry_pos_t>::max() + 1);

public:
    HashIndexBuilder(const std::shared_ptr<FileHandle>& handle,
        std::unique_ptr<InMemFile> overflowFile, uint64_t indexPos,
        common::PhysicalTypeID keyDataType);

public:
    // Reserves space for at least the specified number of elements.
    void bulkReserve(uint32_t numEntries) override;

    bool append(T key, common::offset_t value);
    bool lookup(T key, common::offset_t& result);

    void flush() override;

private:
    Slot<S>* getSlot(const SlotInfo& slotInfo);

    uint32_t allocatePSlots(uint32_t numSlotsToAllocate);
    uint32_t allocateAOSlot();
    /*
     * When a slot is split, we add a new slot, which ends up with an
     * id equal to the slot to split's ID + (1 << header.currentLevel).
     * Values are then rehashed using a hash index which is one bit wider than before,
     * meaning they either stay in the existing slot, or move into the new one.
     */
    void splitSlot(HashIndexHeader& header);

    inline bool equals(T keyToLookup, const S& keyInEntry) const {
        return keyToLookup == keyInEntry;
    }

    inline void insert(
        T key, Slot<S>* slot, uint8_t entryPos, common::offset_t value, uint8_t fingerprint) {
        auto entry = slot->entries[entryPos].data;
        memcpy(entry, &key, sizeof(T));
        memcpy(entry + sizeof(T), &value, sizeof(common::offset_t));
        slot->header.setEntryValid(entryPos, fingerprint);
        KU_ASSERT(HashIndexUtils::getFingerprintForHash(HashIndexUtils::hash(key)) == fingerprint);
    }
    void copy(const uint8_t* oldEntry, slot_id_t newSlotId, uint8_t fingerprint);
    void insertToNewOvfSlot(
        T key, Slot<S>* previousSlot, common::offset_t offset, uint8_t fingerprint);
    common::hash_t hashStored(const S& key) const;

    struct SlotIterator {
        explicit SlotIterator(slot_id_t newSlotId, HashIndexBuilder<T, S>* builder)
            : slotInfo{newSlotId, SlotType::PRIMARY}, slot(builder->getSlot(slotInfo)) {}
        SlotInfo slotInfo;
        Slot<S>* slot;
    };

    inline bool nextChainedSlot(SlotIterator& iter) {
        if (iter.slot->header.nextOvfSlotId != 0) {
            iter.slotInfo.slotId = iter.slot->header.nextOvfSlotId;
            iter.slotInfo.slotType = SlotType::OVF;
            iter.slot = getSlot(iter.slotInfo);
            return true;
        }
        return false;
    }

private:
    std::shared_ptr<FileHandle> fileHandle;
    std::unique_ptr<InMemFile> inMemOverflowFile;
    std::unique_ptr<InMemDiskArrayBuilder<HashIndexHeader>> headerArray;
    std::unique_ptr<InMemDiskArrayBuilder<Slot<S>>> pSlots;
    std::unique_ptr<InMemDiskArrayBuilder<Slot<S>>> oSlots;
    std::unique_ptr<HashIndexHeader> indexHeader;
};

template<>
bool HashIndexBuilder<std::string_view, common::ku_string_t>::equals(
    std::string_view keyToLookup, const common::ku_string_t& keyInEntry) const;

class PrimaryKeyIndexBuilder {
public:
    PrimaryKeyIndexBuilder(const std::string& fName, common::PhysicalTypeID keyDataType,
        common::VirtualFileSystem* vfs);

    void bulkReserve(uint32_t numEntries);

    // Note: append assumes that bulkRserve has been called before it and the index has reserved
    // enough space already.
    template<common::HashablePrimitive T>
    bool append(T key, common::offset_t value) {
        return appendWithIndexPos(key, value, HashIndexUtils::getHashIndexPosition(key));
    }
    bool append(std::string_view key, common::offset_t value) {
        return appendWithIndexPos(key, value, HashIndexUtils::getHashIndexPosition(key));
    }
    template<common::HashablePrimitive T>
    bool appendWithIndexPos(T key, common::offset_t value, uint64_t indexPos) {
        KU_ASSERT(keyDataTypeID == common::TypeUtils::getPhysicalTypeIDForType<T>());
        KU_ASSERT(HashIndexUtils::getHashIndexPosition(key) == indexPos);
        return getTypedHashIndex<T>(indexPos)->append(key, value);
    }
    bool appendWithIndexPos(std::string_view key, common::offset_t value, uint64_t indexPos) {
        KU_ASSERT(keyDataTypeID == common::PhysicalTypeID::STRING);
        KU_ASSERT(HashIndexUtils::getHashIndexPosition(key) == indexPos);
        return getTypedHashIndex<std::string_view, common::ku_string_t>(indexPos)->append(
            key, value);
    }
    template<common::HashablePrimitive T>
    bool lookup(T key, common::offset_t& result) {
        KU_ASSERT(keyDataTypeID == common::TypeUtils::getPhysicalTypeIDForType<T>());
        return getTypedHashIndex<T>(HashIndexUtils::getHashIndexPosition(key))->lookup(key, result);
    }
    bool lookup(std::string_view key, common::offset_t& result) {
        KU_ASSERT(keyDataTypeID == common::PhysicalTypeID::STRING);
        return getTypedHashIndex<std::string_view, common::ku_string_t>(
            HashIndexUtils::getHashIndexPosition(key))
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
    common::PhysicalTypeID keyDataTypeID;
    std::atomic<common::page_idx_t> overflowPageCounter;
    std::vector<std::unique_ptr<InMemHashIndex>> hashIndexBuilders;
};

} // namespace storage
} // namespace kuzu
