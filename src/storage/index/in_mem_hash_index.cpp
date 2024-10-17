#include "storage/index/in_mem_hash_index.h"

#include <cstring>

#include "common/constants.h"
#include "common/types/ku_string.h"
#include "common/types/types.h"
#include "storage/index/hash_index_header.h"
#include "storage/index/hash_index_slot.h"
#include "storage/index/hash_index_utils.h"
#include "storage/storage_structure/disk_array.h"
#include "storage/storage_structure/overflow_file.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

template<typename T>
InMemHashIndex<T>::InMemHashIndex(OverflowFileHandle* overflowFileHandle)
    : overflowFileHandle(overflowFileHandle), pSlots{std::make_unique<BlockVector<Slot<T>>>()},
      oSlots{std::make_unique<BlockVector<Slot<T>>>()}, indexHeader{} {
    // Match HashIndex in allocating at least one page of slots so that we don't split within the
    // same page
    allocateSlots(KUZU_PAGE_SIZE / pSlots->getAlignedElementSize());
}

template<typename T>
void InMemHashIndex<T>::clear() {
    indexHeader = HashIndexHeader();
    pSlots = std::make_unique<BlockVector<Slot<T>>>();
    oSlots = std::make_unique<BlockVector<Slot<T>>>();
    allocateSlots(KUZU_PAGE_SIZE / pSlots->getAlignedElementSize());
}

template<typename T>
void InMemHashIndex<T>::allocateSlots(uint32_t newNumSlots) {
    auto numSlotsOfCurrentLevel = 1u << this->indexHeader.currentLevel;
    while ((numSlotsOfCurrentLevel << 1) <= newNumSlots) {
        this->indexHeader.incrementLevel();
        numSlotsOfCurrentLevel <<= 1;
    }
    if (newNumSlots >= numSlotsOfCurrentLevel) {
        this->indexHeader.nextSplitSlotId = newNumSlots - numSlotsOfCurrentLevel;
    }
    auto existingSlots = pSlots->size();
    if (newNumSlots > existingSlots) {
        allocatePSlots(newNumSlots - existingSlots);
    }
}

template<typename T>
void InMemHashIndex<T>::reserve(uint32_t numEntries_) {
    slot_id_t numRequiredEntries = HashIndexUtils::getNumRequiredEntries(numEntries_);
    auto numRequiredSlots = (numRequiredEntries + getSlotCapacity<T>() - 1) / getSlotCapacity<T>();
    if (numRequiredSlots <= pSlots->size()) {
        return;
    }
    if (indexHeader.numEntries == 0) {
        allocateSlots(numRequiredSlots);
    } else {
        while (pSlots->size() < numRequiredSlots) {
            splitSlot(indexHeader);
        }
    }
}

template<typename T>
void InMemHashIndex<T>::splitSlot(HashIndexHeader& header) {
    // Add new slot
    allocatePSlots(1);

    // Rehash the entries in the slot to split
    SlotIterator originalSlot(header.nextSplitSlotId, this);
    // Use a separate iterator to track the first empty position so that the gapless entries can be
    // maintained
    SlotIterator originalSlotForInsert(header.nextSplitSlotId, this);
    auto entryPosToInsert = 0u;
    SlotIterator newSlot(pSlots->size() - 1, this);
    entry_pos_t newSlotPos = 0;
    bool gaps = false;
    do {
        for (auto entryPos = 0u; entryPos < getSlotCapacity<T>(); entryPos++) {
            if (!originalSlot.slot->header.isEntryValid(entryPos)) {
                // Check that this function leaves no gaps
                KU_ASSERT(originalSlot.slot->header.numEntries() ==
                          std::countr_one(originalSlot.slot->header.validityMask));
                // There should be no gaps, so when we encounter an invalid entry we can return
                // early
                reclaimOverflowSlots(originalSlotForInsert);
                header.incrementNextSplitSlotId();
                return;
            }
            const auto& entry = originalSlot.slot->entries[entryPos];
            const auto& hash = this->hashStored(originalSlot.slot->entries[entryPos].key);
            const auto fingerprint = HashIndexUtils::getFingerprintForHash(hash);
            const auto newSlotId = hash & header.higherLevelHashMask;
            if (newSlotId != header.nextSplitSlotId) {
                if (newSlotPos >= getSlotCapacity<T>()) {
                    auto newOvfSlotId = allocateAOSlot();
                    newSlot.slot->header.nextOvfSlotId = newOvfSlotId;
                    nextChainedSlot(newSlot);
                    newSlotPos = 0;
                }
                newSlot.slot->entries[newSlotPos] = entry;
                newSlot.slot->header.setEntryValid(newSlotPos, fingerprint);
                originalSlot.slot->header.setEntryInvalid(entryPos);
                newSlotPos++;
                gaps = true;
            } else if (gaps) {
                // If we have created a gap previously, move the entry to the first gap to avoid
                // leaving gaps
                while (originalSlotForInsert.slot->header.isEntryValid(entryPosToInsert)) {
                    entryPosToInsert++;
                    if (entryPosToInsert >= getSlotCapacity<T>()) {
                        entryPosToInsert = 0;
                        nextChainedSlot(originalSlotForInsert);
                    }
                }
                originalSlotForInsert.slot->entries[entryPosToInsert] = entry;
                originalSlotForInsert.slot->header.setEntryValid(entryPosToInsert, fingerprint);
                originalSlot.slot->header.setEntryInvalid(entryPos);
            }
        }
        KU_ASSERT(originalSlot.slot->header.numEntries() ==
                  std::countr_one(originalSlot.slot->header.validityMask));
    } while (nextChainedSlot(originalSlot));

    reclaimOverflowSlots(originalSlotForInsert);
    header.incrementNextSplitSlotId();
}

template<typename T>
void InMemHashIndex<T>::reclaimOverflowSlots(SlotIterator iter) {
    // Reclaim empty overflow slots at the end of the chain.
    // This saves the cost of having to iterate over them, and reduces memory usage by letting them
    // be used instead of allocating new slots
    if (iter.slot->header.nextOvfSlotId != SlotHeader::INVALID_OVERFLOW_SLOT_ID) {
        // Skip past the last non-empty entry
        Slot<T>* lastNonEmptySlot = iter.slot;
        while (iter.slot->header.numEntries() > 0 || iter.slotInfo.slotType == SlotType::PRIMARY) {
            lastNonEmptySlot = iter.slot;
            if (!nextChainedSlot(iter)) {
                break;
            }
        }
        lastNonEmptySlot->header.nextOvfSlotId = SlotHeader::INVALID_OVERFLOW_SLOT_ID;
        while (iter.slotInfo != HashIndexUtils::INVALID_OVF_INFO) {
            // Remove empty overflow slots from slot chain
            KU_ASSERT(iter.slot->header.numEntries() == 0);
            auto slotInfo = iter.slotInfo;
            auto slot = clearNextOverflowAndAdvanceIter(iter);
            if (slotInfo.slotType == SlotType::OVF) {
                // Insert empty slot into free slot chain
                KU_ASSERT(slotInfo.slotId != SlotHeader::INVALID_OVERFLOW_SLOT_ID);
                slot->header.nextOvfSlotId = indexHeader.firstFreeOverflowSlotId;
                indexHeader.firstFreeOverflowSlotId = slotInfo.slotId;
            }
        }
    }
}

template<typename T>
Slot<T>* InMemHashIndex<T>::clearNextOverflowAndAdvanceIter(SlotIterator& iter) {
    auto originalSlot = iter.slot;
    auto nextOverflowSlot = iter.slot->header.nextOvfSlotId;
    iter.slot->header.nextOvfSlotId = SlotHeader::INVALID_OVERFLOW_SLOT_ID;
    iter.slotInfo = SlotInfo{nextOverflowSlot, SlotType::OVF};
    if (nextOverflowSlot != SlotHeader::INVALID_OVERFLOW_SLOT_ID) {
        iter.slot = getSlot(iter.slotInfo);
    }
    return originalSlot;
}

template<typename T>
uint32_t InMemHashIndex<T>::allocatePSlots(uint32_t numSlotsToAllocate) {
    auto oldNumSlots = pSlots->size();
    auto newNumSlots = oldNumSlots + numSlotsToAllocate;
    pSlots->resize(newNumSlots);
    return oldNumSlots;
}

template<typename T>
uint32_t InMemHashIndex<T>::allocateAOSlot() {
    if (indexHeader.firstFreeOverflowSlotId == SlotHeader::INVALID_OVERFLOW_SLOT_ID) {
        auto oldNumSlots = oSlots->size();
        auto newNumSlots = oldNumSlots + 1;
        oSlots->resize(newNumSlots);
        return oldNumSlots;
    } else {
        auto freeOSlotId = indexHeader.firstFreeOverflowSlotId;
        auto& slot = (*oSlots)[freeOSlotId];
        // Remove slot from the free slot chain
        indexHeader.firstFreeOverflowSlotId = slot.header.nextOvfSlotId;
        KU_ASSERT(slot.header.numEntries() == 0);
        slot.header.nextOvfSlotId = SlotHeader::INVALID_OVERFLOW_SLOT_ID;
        return freeOSlotId;
    }
}

template<typename T>
Slot<T>* InMemHashIndex<T>::getSlot(const SlotInfo& slotInfo) const {
    if (slotInfo.slotType == SlotType::PRIMARY) {
        return &pSlots->operator[](slotInfo.slotId);
    } else {
        return &oSlots->operator[](slotInfo.slotId);
    }
}

template<typename T>
common::hash_t InMemHashIndex<T>::hashStored(const T& key) const {
    return HashIndexUtils::hash(key);
}

template<>
common::hash_t InMemHashIndex<ku_string_t>::hashStored(const ku_string_t& key) const {
    auto kuString = key;
    return HashIndexUtils::hash(
        overflowFileHandle->readString(transaction::TransactionType::WRITE, kuString));
}

template class InMemHashIndex<int64_t>;
template class InMemHashIndex<int32_t>;
template class InMemHashIndex<int16_t>;
template class InMemHashIndex<int8_t>;
template class InMemHashIndex<uint64_t>;
template class InMemHashIndex<uint32_t>;
template class InMemHashIndex<uint16_t>;
template class InMemHashIndex<uint8_t>;
template class InMemHashIndex<double>;
template class InMemHashIndex<float>;
template class InMemHashIndex<int128_t>;
template class InMemHashIndex<ku_string_t>;

} // namespace storage
} // namespace kuzu
