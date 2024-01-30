#include "storage/index/hash_index_builder.h"

#include <cstring>
#include <optional>

#include "common/type_utils.h"
#include "common/types/ku_string.h"
#include "common/types/types.h"
#include "storage/storage_structure/in_mem_file.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

template<IndexHashable T, typename S>
HashIndexBuilder<T, S>::HashIndexBuilder(const std::shared_ptr<FileHandle>& fileHandle,
    const std::shared_ptr<Mutex<InMemFile>>& overflowFile, uint64_t indexPos,
    PhysicalTypeID keyDataType)
    : BaseHashIndex<T, S>{}, fileHandle(fileHandle),
      inMemOverflowFile(overflowFile), numEntries{0} {
    this->indexHeader = std::make_unique<HashIndexHeader>(keyDataType);
    headerArray = std::make_unique<InMemDiskArrayBuilder<HashIndexHeader>>(*fileHandle,
        NUM_HEADER_PAGES * indexPos + INDEX_HEADER_ARRAY_HEADER_PAGE_IDX, 0 /* numElements */);
    pSlots = std::make_unique<InMemDiskArrayBuilder<Slot<S>>>(
        *fileHandle, NUM_HEADER_PAGES * indexPos + P_SLOTS_HEADER_PAGE_IDX, 0 /* numElements */);
    // Reserve a slot for oSlots, which is always skipped, as we treat slot idx 0 as NULL.
    oSlots = std::make_unique<InMemDiskArrayBuilder<Slot<S>>>(
        *fileHandle, NUM_HEADER_PAGES * indexPos + O_SLOTS_HEADER_PAGE_IDX, 1 /* numElements */);
    allocatePSlots(2);
}

template<IndexHashable T, typename S>
void HashIndexBuilder<T, S>::bulkReserve(uint32_t numEntries_) {
    slot_id_t numRequiredEntries = this->getNumRequiredEntries(numEntries.load(), numEntries_);
    // Build from scratch.
    slotCapacity = getSlotCapacity<S>();
    auto numRequiredSlots = (numRequiredEntries + slotCapacity - 1) / slotCapacity;
    auto numSlotsOfCurrentLevel = 1u << this->indexHeader->currentLevel;
    while ((numSlotsOfCurrentLevel << 1) < numRequiredSlots) {
        this->indexHeader->incrementLevel();
        numSlotsOfCurrentLevel <<= 1;
    }
    if (numRequiredSlots > numSlotsOfCurrentLevel) {
        this->indexHeader->nextSplitSlotId = numRequiredSlots - numSlotsOfCurrentLevel;
    }
    allocatePSlots(numRequiredSlots);
}

template<IndexHashable T, typename S>
bool HashIndexBuilder<T, S>::append(T key, offset_t value) {
    SlotInfo pSlotInfo{this->getPrimarySlotIdForKey(*this->indexHeader, key), SlotType::PRIMARY};
    auto currentSlotInfo = pSlotInfo;
    Slot<S>* currentSlot = nullptr;
    while (currentSlotInfo.slotType == SlotType::PRIMARY || currentSlotInfo.slotId != 0) {
        currentSlot = getSlot(currentSlotInfo);
        if (lookupOrExistsInSlotWithoutLock<false /* exists */>(currentSlot, key)) {
            // Key already exists. No append is allowed.
            return false;
        }
        if (currentSlot->header.numEntries < slotCapacity) {
            break;
        }
        currentSlotInfo.slotId = currentSlot->header.nextOvfSlotId;
        currentSlotInfo.slotType = SlotType::OVF;
    }
    KU_ASSERT(currentSlot);
    insertToSlotWithoutLock(currentSlot, key, value);
    numEntries.fetch_add(1);
    return true;
}

template<IndexHashable T, typename S>
bool HashIndexBuilder<T, S>::lookup(T key, offset_t& result) {
    SlotInfo pSlotInfo{this->getPrimarySlotIdForKey(*this->indexHeader, key), SlotType::PRIMARY};
    SlotInfo currentSlotInfo = pSlotInfo;
    Slot<S>* currentSlot;
    while (currentSlotInfo.slotType == SlotType::PRIMARY || currentSlotInfo.slotId != 0) {
        currentSlot = getSlot(currentSlotInfo);
        if (lookupOrExistsInSlotWithoutLock<true /* lookup */>(currentSlot, key, &result)) {
            return true;
        }
        currentSlotInfo.slotId = currentSlot->header.nextOvfSlotId;
        currentSlotInfo.slotType = SlotType::OVF;
    }
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
template<bool IS_LOOKUP>
bool HashIndexBuilder<T, S>::lookupOrExistsInSlotWithoutLock(
    Slot<S>* slot, T key, offset_t* result) {
    auto guard = inMemOverflowFile ?
                     std::make_optional<MutexGuard<InMemFile>>(inMemOverflowFile->lock()) :
                     std::nullopt;
    auto memFile = guard ? guard->get() : nullptr;
    for (auto entryPos = 0u; entryPos < slot->header.numEntries; entryPos++) {
        auto& entry = slot->entries[entryPos];
        if (this->equals(key, *(S*)entry.data, memFile)) {
            if constexpr (IS_LOOKUP) {
                memcpy(result, entry.data + this->indexHeader->numBytesPerKey, sizeof(offset_t));
            }
            return true;
        }
    }
    return false;
}

template<IndexHashable T, typename S>
void HashIndexBuilder<T, S>::insertToSlotWithoutLock(Slot<S>* slot, T key, offset_t value) {
    if (slot->header.numEntries == slotCapacity) {
        // Allocate a new oSlot and change the nextOvfSlotId.
        auto ovfSlotId = allocateAOSlot();
        slot->header.nextOvfSlotId = ovfSlotId;
        slot = getSlot(SlotInfo{ovfSlotId, SlotType::OVF});
    }
    auto guard = inMemOverflowFile ?
                     std::make_optional<MutexGuard<InMemFile>>(inMemOverflowFile->lock()) :
                     std::nullopt;
    auto memFile = guard ? guard->get() : nullptr;
    for (auto entryPos = 0u; entryPos < slotCapacity; entryPos++) {
        if (!slot->header.isEntryValid(entryPos)) {
            insertNoGuard(key, slot->entries[entryPos].data, value, memFile);
            slot->header.setEntryValid(entryPos);
            slot->header.numEntries++;
            break;
        }
    }
}

template<IndexHashable T, typename S>
void HashIndexBuilder<T, S>::flush() {
    this->indexHeader->numEntries = numEntries.load();
    headerArray->resize(1, true /* setToZero */);
    headerArray->operator[](0) = *this->indexHeader;
    headerArray->saveToDisk();
    pSlots->saveToDisk();
    oSlots->saveToDisk();
    if (this->indexHeader->keyDataTypeID == PhysicalTypeID::STRING) {
        auto guard = inMemOverflowFile->lock();
        guard->flush();
    }
}

template<>
void HashIndexBuilder<std::string_view, ku_string_t>::insert(
    std::string_view key, uint8_t* entry, offset_t offset) {
    auto guard = inMemOverflowFile->lock();
    auto kuString = guard->appendString(key);
    memcpy(entry, &kuString, NUM_BYTES_FOR_STRING_KEY);
    memcpy(entry + NUM_BYTES_FOR_STRING_KEY, &offset, sizeof(common::offset_t));
}

template<>
void HashIndexBuilder<std::string_view, ku_string_t>::insertNoGuard(
    std::string_view key, uint8_t* entry, offset_t offset, InMemFile* inMemOverflowFile) {
    auto kuString = inMemOverflowFile->appendString(key);
    memcpy(entry, &kuString, NUM_BYTES_FOR_STRING_KEY);
    memcpy(entry + NUM_BYTES_FOR_STRING_KEY, &offset, sizeof(common::offset_t));
}

template<IndexHashable T, typename S>
common::hash_t HashIndexBuilder<T, S>::hashStored(
    transaction::TransactionType /*trxType*/, const S& key) const {
    return this->hash(key);
}

template<>
common::hash_t HashIndexBuilder<std::string_view, ku_string_t>::hashStored(
    transaction::TransactionType /*trxType*/, const ku_string_t& key) const {
    auto guard = inMemOverflowFile->lock();
    auto kuString = key;
    return this->hash(guard->readString(&kuString));
}

template<>
bool HashIndexBuilder<std::string_view, ku_string_t>::equals(std::string_view keyToLookup,
    const ku_string_t& keyInEntry, const InMemFile* inMemOverflowFile) {
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
        // For long strings, read overflow values and check if they are true.
        PageCursor cursor;
        TypeUtils::decodeOverflowPtr(keyInEntry.overflowPtr, cursor.pageIdx, cursor.elemPosInPage);
        auto lengthRead = 0u;
        while (lengthRead < keyInEntry.len) {
            auto numBytesToCheckInPage =
                std::min(static_cast<uint64_t>(keyInEntry.len) - lengthRead,
                    BufferPoolConstants::PAGE_4KB_SIZE - cursor.elemPosInPage);
            if (memcmp(keyToLookup.data() + lengthRead,
                    inMemOverflowFile->getPage(cursor.pageIdx)->data + cursor.elemPosInPage,
                    numBytesToCheckInPage) != 0) {
                return false;
            }
            cursor.nextPage();
            lengthRead += numBytesToCheckInPage;
        }
        return true;
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
    : keyDataTypeID{keyDataType} {
    auto fileHandle =
        std::make_shared<FileHandle>(fName, FileHandle::O_PERSISTENT_FILE_CREATE_NOT_EXISTS, vfs);
    fileHandle->addNewPages(NUM_HEADER_PAGES * NUM_HASH_INDEXES);
    if (keyDataType == PhysicalTypeID::STRING) {
        overflowFile = std::make_shared<Mutex<InMemFile>>(
            InMemFile(StorageUtils::getOverflowFileName(fileHandle->getFileInfo()->path), vfs));
    }
    hashIndexBuilders.reserve(NUM_HASH_INDEXES);
    TypeUtils::visit(
        keyDataTypeID,
        [&]<IndexHashable T>(T) {
            if constexpr (std::is_same_v<T, ku_string_t>) {
                for (auto i = 0u; i < NUM_HASH_INDEXES; i++) {
                    hashIndexBuilders.push_back(
                        std::make_unique<HashIndexBuilder<std::string_view, ku_string_t>>(
                            fileHandle, overflowFile, i, keyDataType));
                }
            } else if constexpr (HashablePrimitive<T>) {
                for (auto i = 0u; i < NUM_HASH_INDEXES; i++) {
                    hashIndexBuilders.push_back(std::make_unique<HashIndexBuilder<T>>(
                        fileHandle, overflowFile, i, keyDataType));
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
