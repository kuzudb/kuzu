#pragma once

#include <memory>

#include "common/static_vector.h"
#include "common/types/internal_id_t.h"
#include "common/types/ku_string.h"
#include "common/types/types.h"
#include "storage/index/hash_index_header.h"
#include "storage/index/hash_index_slot.h"
#include "storage/index/hash_index_utils.h"
#include "storage/storage_structure/disk_array.h"
#include "storage/storage_structure/overflow_file.h"

namespace kuzu {
namespace storage {

constexpr size_t BUFFER_SIZE = 1024;
template<typename T>
using IndexBuffer = common::StaticVector<std::pair<T, common::offset_t>, BUFFER_SIZE>;

template<typename T>
using HashIndexType =
    std::conditional_t<std::same_as<T, std::string_view> || std::same_as<T, std::string>,
        common::ku_string_t, T>;

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
class InMemHashIndex final {
    static_assert(getSlotCapacity<T>() <= SlotHeader::FINGERPRINT_CAPACITY);
    // Size of the validity mask
    static_assert(getSlotCapacity<T>() <= sizeof(SlotHeader().validityMask) * 8);
    static_assert(getSlotCapacity<T>() <= std::numeric_limits<entry_pos_t>::max() + 1);
    static_assert(DiskArray<Slot<T>>::getAlignedElementSize() <=
                  common::HashIndexConstants::SLOT_CAPACITY_BYTES);
    static_assert(DiskArray<Slot<T>>::getAlignedElementSize() >= sizeof(Slot<T>));
    static_assert(DiskArray<Slot<T>>::getAlignedElementSize() >
                  common::HashIndexConstants::SLOT_CAPACITY_BYTES / 2);

public:
    explicit InMemHashIndex(OverflowFileHandle* overflowFileHandle);

    static void createEmptyIndexFiles(uint64_t indexPos, FileHandle& fileHandle);

    // Reserves space for at least the specified number of elements.
    // This reserves space for numEntries in total, regardless of existing entries in the builder
    void reserve(uint32_t numEntries);
    // Allocates the given number of new slots, ignoo
    void allocateSlots(uint32_t numSlots);

    using BufferKeyType = std::conditional_t<std::same_as<T, common::ku_string_t>, std::string, T>;
    // TODO(Ben): Ideally, `Key` should reuse `HashIndexType`.
    using Key = std::conditional_t<std::same_as<T, common::ku_string_t>, std::string_view, T>;
    // Appends the buffer to the index. Returns the number of values successfully inserted.
    // I.e. if a key fails to insert, its index will be the return value
    size_t append(const IndexBuffer<BufferKeyType>& buffer) {
        reserve(indexHeader.numEntries + buffer.size());
        // Do both searches after splitting. Returning early if the key already exists isn't a
        // particular concern and doing both after splitting allows the slotID to be reused
        common::hash_t hashes[BUFFER_SIZE];
        for (size_t i = 0; i < buffer.size(); i++) {
            hashes[i] = HashIndexUtils::hash(buffer[i].first);
        }
        for (size_t i = 0; i < buffer.size(); i++) {
            auto& [key, value] = buffer[i];
            if (!appendInternal(key, value, hashes[i])) {
                return i;
            }
        }
        return buffer.size();
    }

    inline bool append(Key key, common::offset_t value) {
        reserve(indexHeader.numEntries + 1);
        return appendInternal(key, value, HashIndexUtils::hash(key));
    }
    bool lookup(Key key, common::offset_t& result) {
        // This needs to be fast if the builder is empty since this function is always tried
        // when looking up in the persistent hash index
        if (this->indexHeader.numEntries == 0) {
            return false;
        }
        auto hashValue = HashIndexUtils::hash(key);
        auto fingerprint = HashIndexUtils::getFingerprintForHash(hashValue);
        auto slotId = HashIndexUtils::getPrimarySlotIdForHash(this->indexHeader, hashValue);
        SlotIterator iter(slotId, this);
        auto entryPos = findEntry(iter, key, fingerprint);
        if (entryPos != SlotHeader::INVALID_ENTRY_POS) {
            result = iter.slot->entries[entryPos].value;
            return true;
        }
        return false;
    }

    uint64_t size() const { return this->indexHeader.numEntries; }
    bool empty() const { return size() == 0; }

    void clear();

    struct SlotIterator {
        explicit SlotIterator(slot_id_t newSlotId, const InMemHashIndex<T>* builder)
            : slotInfo{newSlotId, SlotType::PRIMARY}, slot(builder->getSlot(slotInfo)) {}
        SlotInfo slotInfo;
        Slot<T>* slot;
    };

    // Leaves the slot pointer pointing at the last slot to make it easier to add a new one
    inline bool nextChainedSlot(SlotIterator& iter) const {
        iter.slotInfo.slotId = iter.slot->header.nextOvfSlotId;
        iter.slotInfo.slotType = SlotType::OVF;
        if (iter.slot->header.nextOvfSlotId != SlotHeader::INVALID_OVERFLOW_SLOT_ID) {
            iter.slot = getSlot(iter.slotInfo);
            return true;
        }
        return false;
    }

    inline uint64_t numPrimarySlots() const { return pSlots->size(); }
    inline uint64_t numOverflowSlots() const { return oSlots->size(); }

    inline const HashIndexHeader& getIndexHeader() const { return indexHeader; }

    // Deletes key, maintaining gapless structure by replacing it with the last entry in the
    // slot
    bool deleteKey(Key key) {
        if (this->indexHeader.numEntries == 0) {
            return false;
        }
        auto hashValue = HashIndexUtils::hash(key);
        auto fingerprint = HashIndexUtils::getFingerprintForHash(hashValue);
        auto slotId = HashIndexUtils::getPrimarySlotIdForHash(this->indexHeader, hashValue);
        SlotIterator iter(slotId, this);
        std::optional<entry_pos_t> deletedPos = 0;
        do {
            for (auto entryPos = 0u; entryPos < getSlotCapacity<T>(); entryPos++) {
                if (iter.slot->header.isEntryValid(entryPos) &&
                    iter.slot->header.fingerprints[entryPos] == fingerprint &&
                    equals(key, iter.slot->entries[entryPos].key)) {
                    deletedPos = entryPos;
                    iter.slot->header.setEntryInvalid(entryPos);
                    break;
                }
            }
            if (deletedPos.has_value()) {
                break;
            }
        } while (nextChainedSlot(iter));

        if (deletedPos.has_value()) {
            // Find the last valid entry and move it into the deleted position
            auto newIter = iter;
            while (nextChainedSlot(newIter))
                ;
            if (newIter.slotInfo != iter.slotInfo ||
                *deletedPos != newIter.slot->header.numEntries() - 1) {
                auto lastEntryPos = newIter.slot->header.numEntries();
                iter.slot->entries[*deletedPos] = newIter.slot->entries[lastEntryPos];
                iter.slot->header.setEntryValid(*deletedPos,
                    newIter.slot->header.fingerprints[lastEntryPos]);
                newIter.slot->header.setEntryInvalid(lastEntryPos);
            }
        }
        return false;
    }

private:
    // Assumes that space has already been allocated for the entry
    bool appendInternal(Key key, common::offset_t value, common::hash_t hash) {
        auto fingerprint = HashIndexUtils::getFingerprintForHash(hash);
        auto slotID = HashIndexUtils::getPrimarySlotIdForHash(this->indexHeader, hash);
        SlotIterator iter(slotID, this);
        // The builder never keeps holes and doesn't support deletions
        // Check the valid entries, then insert at the end if we don't find one which matches
        auto entryPos = findEntry(iter, key, fingerprint);
        auto numEntries = iter.slot->header.numEntries();
        if (entryPos != SlotHeader::INVALID_ENTRY_POS) {
            // The key already exists
            return false;
        } else if (numEntries < getSlotCapacity<T>()) [[likely]] {
            // The key does not exist and the last slot has free space
            insert(key, iter.slot, numEntries, value, fingerprint);
            this->indexHeader.numEntries++;
            return true;
        }
        // The last slot is full. Insert a new one
        insertToNewOvfSlot(key, iter.slot, value, fingerprint);
        this->indexHeader.numEntries++;
        return true;
    }
    Slot<T>* getSlot(const SlotInfo& slotInfo) const;

    uint32_t allocatePSlots(uint32_t numSlotsToAllocate);
    uint32_t allocateAOSlot();
    /*
     * When a slot is split, we add a new slot, which ends up with an
     * id equal to the slot to split's ID + (1 << header.currentLevel).
     * Values are then rehashed using a hash index which is one bit wider than before,
     * meaning they either stay in the existing slot, or move into the new one.
     */
    void splitSlot(HashIndexHeader& header);
    // Reclaims empty overflow slots to be re-used, starting from the given slot iterator
    void reclaimOverflowSlots(SlotIterator iter);

    inline bool equals(Key keyToLookup, const T& keyInEntry) const {
        if constexpr (std::same_as<T, common::ku_string_t>) {
            // Checks if prefix and len matches first.
            if (!HashIndexUtils::areStringPrefixAndLenEqual(keyToLookup, keyInEntry)) {
                return false;
            }
            if (keyInEntry.len <= common::ku_string_t::PREFIX_LENGTH) {
                // For strings shorter than PREFIX_LENGTH, the result must be true.
                return true;
            } else if (keyInEntry.len <= common::ku_string_t::SHORT_STR_LENGTH) {
                // For short strings, whose lengths are larger than PREFIX_LENGTH, check if their
                // actual values are equal.
                return memcmp(keyToLookup.data(), keyInEntry.prefix, keyInEntry.len) == 0;
            } else {
                // For long strings, compare with overflow data
                return overflowFileHandle->equals(transaction::TransactionType::WRITE, keyToLookup,
                    keyInEntry);
            }
        } else {
            return keyToLookup == keyInEntry;
        }
    }

    inline void insert(Key key, Slot<T>* slot, uint8_t entryPos, common::offset_t value,
        uint8_t fingerprint) {
        KU_ASSERT(HashIndexUtils::getFingerprintForHash(HashIndexUtils::hash(key)) == fingerprint);
        auto& entry = slot->entries[entryPos];
        if constexpr (std::same_as<T, common::ku_string_t>) {
            entry = SlotEntry<common::ku_string_t>(overflowFileHandle->writeString(key), value);
            slot->header.setEntryValid(entryPos, fingerprint);
        } else {
            entry = SlotEntry<T>(key, value);
            slot->header.setEntryValid(entryPos, fingerprint);
        }
    }
    void copy(const SlotEntry<T>& oldEntry, slot_id_t newSlotId, uint8_t fingerprint);

    void insertToNewOvfSlot(Key key, Slot<T>* previousSlot, common::offset_t offset,
        uint8_t fingerprint) {
        auto newSlotId = allocateAOSlot();
        previousSlot->header.nextOvfSlotId = newSlotId;
        auto newSlot = getSlot(SlotInfo{newSlotId, SlotType::OVF});
        auto entryPos = 0u; // Always insert to the first entry when there is a new slot.
        insert(key, newSlot, entryPos, offset, fingerprint);
    }

    common::hash_t hashStored(const T& key) const;
    Slot<T>* clearNextOverflowAndAdvanceIter(SlotIterator& iter);

    // Finds the entry matching the given key. The iterator will be advanced and will either point
    // to the slot containing the matching entry, or the last slot available
    entry_pos_t findEntry(SlotIterator& iter, Key key, uint8_t fingerprint) {
        do {
            auto numEntries = iter.slot->header.numEntries();
            KU_ASSERT(numEntries == std::countr_one(iter.slot->header.validityMask));
            for (auto entryPos = 0u; entryPos < numEntries; entryPos++) {
                if (iter.slot->header.fingerprints[entryPos] == fingerprint &&
                    equals(key, iter.slot->entries[entryPos].key)) [[unlikely]] {
                    // Value already exists
                    return entryPos;
                }
            }
            if (numEntries < getSlotCapacity<T>()) {
                return SlotHeader::INVALID_ENTRY_POS;
            }
        } while (nextChainedSlot(iter));
        return SlotHeader::INVALID_ENTRY_POS;
    }

private:
    // TODO: might be more efficient to use a vector for each slot since this is now only needed
    // in-memory and it would remove the need to handle overflow slots.
    OverflowFileHandle* overflowFileHandle;
    std::unique_ptr<BlockVector<Slot<T>>> pSlots;
    std::unique_ptr<BlockVector<Slot<T>>> oSlots;
    HashIndexHeader indexHeader;
};

} // namespace storage
} // namespace kuzu
