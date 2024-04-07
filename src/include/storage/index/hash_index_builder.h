#pragma once

#include "common/static_vector.h"
#include "common/type_utils.h"
#include "common/types/ku_string.h"
#include "common/types/types.h"
#include "storage/index/hash_index_header.h"
#include "storage/index/hash_index_slot.h"
#include "storage/index/hash_index_utils.h"
#include "storage/storage_structure/disk_array.h"
#include "storage/storage_structure/overflow_file.h"

namespace kuzu {
namespace storage {

class InMemHashIndex {
public:
    virtual ~InMemHashIndex() = default;
    virtual void flush() = 0;
    virtual void bulkReserve(uint32_t numEntries) = 0;
};

constexpr size_t BUFFER_SIZE = 1024;
template<typename T>
using IndexBuffer = common::StaticVector<std::pair<T, common::offset_t>, BUFFER_SIZE>;

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

// T is the key type stored in the slots.
// For strings this is different than the type used when inserting/searching
// (see BufferKeyType and Key)
template<typename T>
class HashIndexBuilder final : public InMemHashIndex {
    static_assert(getSlotCapacity<T>() <= SlotHeader::FINGERPRINT_CAPACITY);
    // Size of the validity mask
    static_assert(getSlotCapacity<T>() <= sizeof(SlotHeader().validityMask) * 8);
    static_assert(getSlotCapacity<T>() <= std::numeric_limits<entry_pos_t>::max() + 1);

public:
    HashIndexBuilder(const std::shared_ptr<FileHandle>& handle,
        OverflowFileHandle* overflowFileHandle, uint64_t indexPos,
        common::PhysicalTypeID keyDataType);

public:
    // Reserves space for at least the specified number of elements.
    void bulkReserve(uint32_t numEntries) override;

    using BufferKeyType =
        typename std::conditional<std::same_as<T, common::ku_string_t>, std::string, T>::type;
    // Appends the buffer to the index. Returns the number of values successfully inserted.
    // I.e. if a key fails to insert, its index will be the return value
    size_t append(const IndexBuffer<BufferKeyType>& buffer);
    using Key =
        typename std::conditional<std::same_as<T, common::ku_string_t>, std::string_view, T>::type;
    bool lookup(Key key, common::offset_t& result);

    void flush() override;

private:
    // Assumes that space has already been allocated for the entry
    bool appendInternal(Key key, common::offset_t value, common::hash_t hash);
    Slot<T>* getSlot(const SlotInfo& slotInfo);

    uint32_t allocatePSlots(uint32_t numSlotsToAllocate);
    uint32_t allocateAOSlot();
    /*
     * When a slot is split, we add a new slot, which ends up with an
     * id equal to the slot to split's ID + (1 << header.currentLevel).
     * Values are then rehashed using a hash index which is one bit wider than before,
     * meaning they either stay in the existing slot, or move into the new one.
     */
    void splitSlot(HashIndexHeader& header);

    inline bool equals(Key keyToLookup, const T& keyInEntry) const {
        return keyToLookup == keyInEntry;
    }

    inline void insert(Key key, Slot<T>* slot, uint8_t entryPos, common::offset_t value,
        uint8_t fingerprint) {
        auto& entry = slot->entries[entryPos];
        entry.key = key;
        entry.value = value;
        slot->header.setEntryValid(entryPos, fingerprint);
        KU_ASSERT(HashIndexUtils::getFingerprintForHash(HashIndexUtils::hash(key)) == fingerprint);
    }
    void copy(const uint8_t* oldEntry, slot_id_t newSlotId, uint8_t fingerprint);
    void insertToNewOvfSlot(Key key, Slot<T>* previousSlot, common::offset_t offset,
        uint8_t fingerprint);
    common::hash_t hashStored(const T& key) const;

    struct SlotIterator {
        explicit SlotIterator(slot_id_t newSlotId, HashIndexBuilder<T>* builder)
            : slotInfo{newSlotId, SlotType::PRIMARY}, slot(builder->getSlot(slotInfo)) {}
        SlotInfo slotInfo;
        Slot<T>* slot;
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
    OverflowFileHandle* overflowFileHandle;
    std::unique_ptr<InMemDiskArrayBuilder<HashIndexHeader>> headerArray;
    std::unique_ptr<InMemDiskArrayBuilder<Slot<T>>> pSlots;
    std::unique_ptr<InMemDiskArrayBuilder<Slot<T>>> oSlots;
    std::unique_ptr<HashIndexHeader> indexHeader;
};

template<>
bool HashIndexBuilder<common::ku_string_t>::equals(std::string_view keyToLookup,
    const common::ku_string_t& keyInEntry) const;

class PrimaryKeyIndexBuilder {
public:
    PrimaryKeyIndexBuilder(const std::string& fName, common::PhysicalTypeID keyDataType,
        common::VirtualFileSystem* vfs);

    void bulkReserve(uint32_t numEntries);

    // Appends the buffer to the index. Returns the number of values successfully inserted.
    // I.e. if a key fails to insert, its index will be the return value
    template<common::IndexHashable T>
    size_t appendWithIndexPos(const IndexBuffer<T>& buffer, uint64_t indexPos) {
        KU_ASSERT(keyDataTypeID == common::TypeUtils::getPhysicalTypeIDForType<T>());
        KU_ASSERT(std::all_of(buffer.begin(), buffer.end(), [&](auto& elem) {
            return HashIndexUtils::getHashIndexPosition(elem.first) == indexPos;
        }));
        return getTypedHashIndex<T>(indexPos)->append(buffer);
    }
    template<common::HashablePrimitive T>
    bool lookup(T key, common::offset_t& result) {
        KU_ASSERT(keyDataTypeID == common::TypeUtils::getPhysicalTypeIDForType<T>());
        return getTypedHashIndex<T>(HashIndexUtils::getHashIndexPosition(key))->lookup(key, result);
    }
    bool lookup(std::string_view key, common::offset_t& result) {
        KU_ASSERT(keyDataTypeID == common::PhysicalTypeID::STRING);
        return getTypedHashIndex<common::ku_string_t>(HashIndexUtils::getHashIndexPosition(key))
            ->lookup(key, result);
    }

    // Not thread safe.
    void flush();

    common::PhysicalTypeID keyTypeID() const { return keyDataTypeID; }

private:
    template<typename T>
    using HashIndexType =
        typename std::conditional<std::same_as<T, std::string_view> || std::same_as<T, std::string>,
            common::ku_string_t, T>::type;
    template<typename T>
    inline HashIndexBuilder<HashIndexType<T>>* getTypedHashIndex(uint64_t indexPos) {
        if constexpr (std::same_as<HashIndexType<T>, common::ku_string_t>) {
            return common::ku_dynamic_cast<InMemHashIndex*, HashIndexBuilder<common::ku_string_t>*>(
                hashIndexBuilders[indexPos].get());
        } else {
            return common::ku_dynamic_cast<InMemHashIndex*, HashIndexBuilder<T>*>(
                hashIndexBuilders[indexPos].get());
        }
    }

private:
    common::PhysicalTypeID keyDataTypeID;
    std::unique_ptr<OverflowFile> overflowFile;
    std::vector<std::unique_ptr<InMemHashIndex>> hashIndexBuilders;
};

} // namespace storage
} // namespace kuzu
