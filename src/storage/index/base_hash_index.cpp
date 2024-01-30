#include "storage/index/base_hash_index.h"

#include "common/types/int128_t.h"
#include "common/types/ku_string.h"
#include "transaction/transaction.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

template<common::IndexHashable T, typename S>
slot_id_t BaseHashIndex<T, S>::getPrimarySlotIdForKey(
    const HashIndexHeader& indexHeader_, T key) const {
    auto hash = this->hash(key);
    auto slotId = hash & indexHeader_.levelHashMask;
    if (slotId < indexHeader_.nextSplitSlotId) {
        slotId = hash & indexHeader_.higherLevelHashMask;
    }
    return slotId;
}

template<common::IndexHashable T, typename S>
void BaseHashIndex<T, S>::loopChainedSlotsToFindOneWithFreeSpace(
    SlotInfo& slotInfo, Slot<S>& slot) {
    while (slotInfo.slotType == SlotType::PRIMARY || slotInfo.slotId > 0) {
        slot = getSlot(TransactionType::WRITE, slotInfo);
        if (slot.header.numEntries < slotCapacity || slot.header.nextOvfSlotId == 0) {
            // Found a slot with empty space.
            break;
        }
        slotInfo.slotId = slot.header.nextOvfSlotId;
        slotInfo.slotType = SlotType::OVF;
    }
}

template<common::IndexHashable T, typename S>
void BaseHashIndex<T, S>::splitSlot(HashIndexHeader& header) {
    appendPSlot();
    rehashSlots(header);
    header.incrementNextSplitSlotId();
}

template<common::IndexHashable T, typename S>
void BaseHashIndex<T, S>::rehashSlots(HashIndexHeader& header) {
    auto slotsToSplit = getChainedSlots(header.nextSplitSlotId);
    for (auto& [slotInfo, slot] : slotsToSplit) {
        auto slotHeader = slot.header;
        slot.header.reset();
        updateSlot(slotInfo, slot);
        for (auto entryPos = 0u; entryPos < slotCapacity; entryPos++) {
            if (!slotHeader.isEntryValid(entryPos)) {
                continue; // Skip invalid entries.
            }
            auto key = (S*)slot.entries[entryPos].data;
            hash_t hash = this->hashStored(TransactionType::WRITE, *key);
            auto newSlotId = hash & header.higherLevelHashMask;
            copyEntryToSlot(newSlotId, *key);
        }
    }
}

template<common::IndexHashable T, typename S>
void BaseHashIndex<T, S>::copyEntryToSlot(slot_id_t slotId, const S& entry) {
    SlotInfo slotInfo{slotId, SlotType::PRIMARY};
    Slot<S> slot;
    loopChainedSlotsToFindOneWithFreeSpace(slotInfo, slot);
    copyKVOrEntryToSlot<const S&, true /* copy entry */>(slotInfo, slot, entry, UINT32_MAX);
    updateSlot(slotInfo, slot);
}

template<common::IndexHashable T, typename S>
std::vector<std::pair<SlotInfo, Slot<S>>> BaseHashIndex<T, S>::getChainedSlots(slot_id_t pSlotId) {
    std::vector<std::pair<SlotInfo, Slot<S>>> slots;
    SlotInfo slotInfo{pSlotId, SlotType::PRIMARY};
    while (slotInfo.slotType == SlotType::PRIMARY || slotInfo.slotId != 0) {
        auto slot = getSlot(TransactionType::WRITE, slotInfo);
        slots.emplace_back(slotInfo, slot);
        slotInfo.slotId = slot.header.nextOvfSlotId;
        slotInfo.slotType = SlotType::OVF;
    }
    return slots;
}

template class BaseHashIndex<int64_t>;
template class BaseHashIndex<int32_t>;
template class BaseHashIndex<int16_t>;
template class BaseHashIndex<int8_t>;
template class BaseHashIndex<uint64_t>;
template class BaseHashIndex<uint32_t>;
template class BaseHashIndex<uint16_t>;
template class BaseHashIndex<uint8_t>;
template class BaseHashIndex<double>;
template class BaseHashIndex<float>;
template class BaseHashIndex<int128_t>;
template class BaseHashIndex<std::string_view, ku_string_t>;
} // namespace storage
} // namespace kuzu
