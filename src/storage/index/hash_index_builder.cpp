#include "storage/index/hash_index_builder.h"

#include <fcntl.h>

#include <cstring>

#include "common/type_utils.h"
#include "common/types/ku_string.h"
#include "common/types/types.h"
#include "storage/index/hash_index_slot.h"
#include "storage/index/hash_index_utils.h"
#include "storage/storage_structure/in_mem_file.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

template<IndexHashable T, typename S>
HashIndexBuilder<T, S>::HashIndexBuilder(const std::shared_ptr<FileHandle>& fileHandle,
    std::unique_ptr<InMemFile> overflowFile, uint64_t indexPos, PhysicalTypeID keyDataType)
    : fileHandle(fileHandle), inMemOverflowFile(std::move(overflowFile)) {
    this->indexHeader = std::make_unique<HashIndexHeader>(keyDataType);
    headerArray = std::make_unique<InMemDiskArrayBuilder<HashIndexHeader>>(*fileHandle,
        NUM_HEADER_PAGES * indexPos + INDEX_HEADER_ARRAY_HEADER_PAGE_IDX, 0 /* numElements */);
    pSlots = std::make_unique<InMemDiskArrayBuilder<Slot<S>>>(
        *fileHandle, NUM_HEADER_PAGES * indexPos + P_SLOTS_HEADER_PAGE_IDX, 0 /* numElements */);
    // Reserve a slot for oSlots, which is always skipped, as we treat slot idx 0 as NULL.
    oSlots = std::make_unique<InMemDiskArrayBuilder<Slot<S>>>(
        *fileHandle, NUM_HEADER_PAGES * indexPos + O_SLOTS_HEADER_PAGE_IDX, 1 /* numElements */);
    allocatePSlots(1u << this->indexHeader->currentLevel);
}

template<IndexHashable T, typename S>
void HashIndexBuilder<T, S>::bulkReserve(uint32_t numEntries_) {
    slot_id_t numRequiredEntries =
        HashIndexUtils::getNumRequiredEntries(this->indexHeader->numEntries, numEntries_);
    // Build from scratch.
    auto numRequiredSlots = (numRequiredEntries + getSlotCapacity<S>() - 1) / getSlotCapacity<S>();
    auto numSlotsOfCurrentLevel = 1u << this->indexHeader->currentLevel;
    while ((numSlotsOfCurrentLevel << 1) < numRequiredSlots) {
        this->indexHeader->incrementLevel();
        numSlotsOfCurrentLevel <<= 1;
    }
    if (numRequiredSlots > numSlotsOfCurrentLevel) {
        this->indexHeader->nextSplitSlotId = numRequiredSlots - numSlotsOfCurrentLevel;
    }
    auto existingSlots = pSlots->getNumElements();
    if (numRequiredSlots > existingSlots) {
        allocatePSlots(numRequiredSlots - existingSlots);
    }
}

template<common::IndexHashable T, typename S>
void HashIndexBuilder<T, S>::copy(
    const uint8_t* oldEntry, slot_id_t newSlotId, uint8_t fingerprint) {
    SlotIterator iter(newSlotId, this);
    do {
        for (auto newEntryPos = 0u; newEntryPos < getSlotCapacity<S>(); newEntryPos++) {
            if (!iter.slot->header.isEntryValid(newEntryPos)) {
                // The original slot was marked as unused, but
                // copying to the original slot is unnecessary and will cause undefined behaviour
                if (oldEntry != iter.slot->entries[newEntryPos].data) {
                    memcpy(iter.slot->entries[newEntryPos].data, oldEntry,
                        this->indexHeader->numBytesPerEntry);
                }
                iter.slot->header.setEntryValid(newEntryPos, fingerprint);
                return;
            }
        }
    } while (nextChainedSlot(iter));
    // Didn't find an available entry in an existing slot. Insert a new overflow slot
    auto newOvfSlotId = allocateAOSlot();
    iter.slot->header.nextOvfSlotId = newOvfSlotId;
    auto newOvfSlot = getSlot(SlotInfo{newOvfSlotId, SlotType::OVF});
    auto newEntryPos = 0u; // Always insert to the first entry when there is a new slot.
    memcpy(newOvfSlot->entries[newEntryPos].data, oldEntry, this->indexHeader->numBytesPerEntry);
    newOvfSlot->header.setEntryValid(newEntryPos, fingerprint);
}

template<common::IndexHashable T, typename S>
void HashIndexBuilder<T, S>::splitSlot(HashIndexHeader& header) {
    // Add new slot
    allocatePSlots(1);

    // Rehash the entries in the slot to split
    SlotIterator iter(header.nextSplitSlotId, this);
    do {
        // Keep a copy of the header so we know which entries were valid
        SlotHeader slotHeader = iter.slot->header;
        // Reset everything except the next overflow id so we can reuse the overflow slot
        iter.slot->header.reset();
        iter.slot->header.nextOvfSlotId = slotHeader.nextOvfSlotId;
        for (auto entryPos = 0u; entryPos < getSlotCapacity<S>(); entryPos++) {
            if (!slotHeader.isEntryValid(entryPos)) {
                continue; // Skip invalid entries.
            }
            const auto* data = (iter.slot->entries[entryPos].data);
            hash_t hash = this->hashStored(*reinterpret_cast<const S*>(data));
            auto fingerprint = HashIndexUtils::getFingerprintForHash(hash);
            auto newSlotId = hash & header.higherLevelHashMask;
            copy(data, newSlotId, fingerprint);
        }
    } while (nextChainedSlot(iter));

    header.incrementNextSplitSlotId();
}

template<IndexHashable T, typename S>
bool HashIndexBuilder<T, S>::append(T key, offset_t value) {
    slot_id_t numRequiredEntries =
        HashIndexUtils::getNumRequiredEntries(this->indexHeader->numEntries, 1);
    while (numRequiredEntries > pSlots->getNumElements() * getSlotCapacity<S>()) {
        this->splitSlot(*this->indexHeader);
    }
    // Do both searches after splitting. Returning early if the key already exists isn't a
    // particular concern and doing both after splitting allows the slotID to be reused
    auto hashValue = HashIndexUtils::hash(key);
    auto fingerprint = HashIndexUtils::getFingerprintForHash(hashValue);
    auto slotID = HashIndexUtils::getPrimarySlotIdForHash(*this->indexHeader, hashValue);
    SlotIterator iter(slotID, this);
    do {
        for (auto entryPos = 0u; entryPos < getSlotCapacity<S>(); entryPos++) {
            if (!iter.slot->header.isEntryValid(entryPos)) {
                // Insert to this position
                // The builder never keeps holes and doesn't support deletions, so this must be the
                // end of the valid entries in this primary slot and the entry does not already
                // exist
                insert(key, iter.slot, entryPos, value, fingerprint);
                this->indexHeader->numEntries++;
                return true;
            } else if (iter.slot->header.fingerprints[entryPos] == fingerprint &&
                       equals(key, *(S*)iter.slot->entries[entryPos].data)) {
                // Value already exists
                return false;
            }
        }
    } while (nextChainedSlot(iter));
    // Didn't find an available slot. Insert a new one
    insertToNewOvfSlot(key, iter.slot, value, fingerprint);
    this->indexHeader->numEntries++;
    return true;
}

template<IndexHashable T, typename S>
bool HashIndexBuilder<T, S>::lookup(T key, offset_t& result) {
    auto hashValue = HashIndexUtils::hash(key);
    auto fingerprint = HashIndexUtils::getFingerprintForHash(hashValue);
    auto slotId = HashIndexUtils::getPrimarySlotIdForHash(*this->indexHeader, hashValue);
    SlotIterator iter(slotId, this);
    do {
        for (auto entryPos = 0u; entryPos < getSlotCapacity<S>(); entryPos++) {
            if (iter.slot->header.isEntryValid(entryPos) &&
                iter.slot->header.fingerprints[entryPos] == fingerprint &&
                equals(key, *(S*)iter.slot->entries[entryPos].data)) {
                // Value already exists
                result = *(common::offset_t*)(iter.slot->entries[entryPos].data +
                                              this->indexHeader->numBytesPerKey);
                return true;
            }
        }
    } while (nextChainedSlot(iter));
    return false;
}

template<IndexHashable T, typename S>
uint32_t HashIndexBuilder<T, S>::allocatePSlots(uint32_t numSlotsToAllocate) {
    auto oldNumSlots = pSlots->getNumElements();
    auto newNumSlots = oldNumSlots + numSlotsToAllocate;
    pSlots->resize(newNumSlots, true /* setToZero */);
    return oldNumSlots;
}

template<IndexHashable T, typename S>
uint32_t HashIndexBuilder<T, S>::allocateAOSlot() {
    auto oldNumSlots = oSlots->getNumElements();
    auto newNumSlots = oldNumSlots + 1;
    oSlots->resize(newNumSlots, true /* setToZero */);
    return oldNumSlots;
}

template<IndexHashable T, typename S>
Slot<S>* HashIndexBuilder<T, S>::getSlot(const SlotInfo& slotInfo) {
    if (slotInfo.slotType == SlotType::PRIMARY) {
        return &pSlots->operator[](slotInfo.slotId);
    } else {
        return &oSlots->operator[](slotInfo.slotId);
    }
}

template<IndexHashable T, typename S>
void HashIndexBuilder<T, S>::flush() {
    headerArray->resize(1, true /* setToZero */);
    headerArray->operator[](0) = *this->indexHeader;
    headerArray->saveToDisk();
    pSlots->saveToDisk();
    oSlots->saveToDisk();
    if constexpr (std::is_same_v<S, ku_string_t>) {
        inMemOverflowFile->flush();
    }
}

template<IndexHashable T, typename S>
inline void HashIndexBuilder<T, S>::insertToNewOvfSlot(
    T key, Slot<S>* previousSlot, common::offset_t offset, uint8_t fingerprint) {
    auto newSlotId = allocateAOSlot();
    previousSlot->header.nextOvfSlotId = newSlotId;
    auto newSlot = getSlot(SlotInfo{newSlotId, SlotType::OVF});
    auto entryPos = 0u; // Always insert to the first entry when there is a new slot.
    insert(key, newSlot, entryPos, offset, fingerprint);
}

template<>
void HashIndexBuilder<std::string_view, ku_string_t>::insert(std::string_view key,
    Slot<ku_string_t>* slot, uint8_t entryPos, offset_t offset, uint8_t fingerprint) {
    auto entry = slot->entries[entryPos].data;
    inMemOverflowFile->appendString(key, *(ku_string_t*)entry);
    memcpy(entry + NUM_BYTES_FOR_STRING_KEY, &offset, sizeof(common::offset_t));
    slot->header.setEntryValid(entryPos, fingerprint);
}

template<IndexHashable T, typename S>
common::hash_t HashIndexBuilder<T, S>::hashStored(const S& key) const {
    return HashIndexUtils::hash(key);
}

template<>
common::hash_t HashIndexBuilder<std::string_view, ku_string_t>::hashStored(
    const ku_string_t& key) const {
    auto kuString = key;
    return HashIndexUtils::hash(inMemOverflowFile->readString(&kuString));
}

template<>
bool HashIndexBuilder<std::string_view, ku_string_t>::equals(
    std::string_view keyToLookup, const ku_string_t& keyInEntry) const {
    // Checks if prefix and len matches first.
    if (!HashIndexUtils::areStringPrefixAndLenEqual(keyToLookup, keyInEntry)) {
        return false;
    }
    if (keyInEntry.len <= ku_string_t::PREFIX_LENGTH) {
        // For strings shorter than PREFIX_LENGTH, the result must be true.
        return true;
    } else if (keyInEntry.len <= ku_string_t::SHORT_STR_LENGTH) {
        // For short strings, whose lengths are larger than PREFIX_LENGTH, check if their actual
        // values are equal.
        return memcmp(keyToLookup.data(), keyInEntry.prefix, keyInEntry.len) == 0;
    } else {
        // For long strings, compare with overflow data
        return inMemOverflowFile->equals(keyToLookup, keyInEntry);
    }
}

template class HashIndexBuilder<int64_t>;
template class HashIndexBuilder<int32_t>;
template class HashIndexBuilder<int16_t>;
template class HashIndexBuilder<int8_t>;
template class HashIndexBuilder<uint64_t>;
template class HashIndexBuilder<uint32_t>;
template class HashIndexBuilder<uint16_t>;
template class HashIndexBuilder<uint8_t>;
template class HashIndexBuilder<double>;
template class HashIndexBuilder<float>;
template class HashIndexBuilder<int128_t>;
template class HashIndexBuilder<std::string_view, ku_string_t>;

PrimaryKeyIndexBuilder::PrimaryKeyIndexBuilder(
    const std::string& fName, PhysicalTypeID keyDataType, VirtualFileSystem* vfs)
    : keyDataTypeID{keyDataType}, overflowPageCounter(0) {
    auto fileHandle =
        std::make_shared<FileHandle>(fName, FileHandle::O_PERSISTENT_FILE_CREATE_NOT_EXISTS, vfs);
    fileHandle->addNewPages(NUM_HEADER_PAGES * NUM_HASH_INDEXES);
    hashIndexBuilders.reserve(NUM_HASH_INDEXES);
    TypeUtils::visit(
        keyDataTypeID,
        [&]<IndexHashable T>(T) {
            if constexpr (std::is_same_v<T, ku_string_t>) {
                auto overflowFileInfo = std::shared_ptr(vfs->openFile(
                    StorageUtils::getOverflowFileName(fileHandle->getFileInfo()->path),
                    O_CREAT | O_WRONLY));
                for (auto i = 0u; i < NUM_HASH_INDEXES; i++) {
                    auto overflowFile =
                        std::make_unique<InMemFile>(overflowFileInfo, overflowPageCounter);
                    hashIndexBuilders.push_back(
                        std::make_unique<HashIndexBuilder<std::string_view, ku_string_t>>(
                            fileHandle, std::move(overflowFile), i, keyDataType));
                }
            } else if constexpr (HashablePrimitive<T>) {
                for (auto i = 0u; i < NUM_HASH_INDEXES; i++) {
                    hashIndexBuilders.push_back(std::make_unique<HashIndexBuilder<T>>(
                        fileHandle, std::unique_ptr<InMemFile>(), i, keyDataType));
                }
            } else {
                KU_UNREACHABLE;
            }
        },
        [](auto) { KU_UNREACHABLE; });
}

void PrimaryKeyIndexBuilder::bulkReserve(uint32_t numEntries) {
    uint32_t eachSize = numEntries / NUM_HASH_INDEXES + 1;
    for (auto i = 0u; i < NUM_HASH_INDEXES; i++) {
        hashIndexBuilders[i]->bulkReserve(eachSize);
    }
}

void PrimaryKeyIndexBuilder::flush() {
    for (auto i = 0u; i < NUM_HASH_INDEXES; i++) {
        hashIndexBuilders[i]->flush();
    }
}

} // namespace storage
} // namespace kuzu
