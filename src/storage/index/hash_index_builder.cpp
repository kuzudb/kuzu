#include "storage/index/hash_index_builder.h"

namespace kuzu {
namespace storage {

slot_id_t BaseHashIndex::getPrimarySlotIdForKey(
    const HashIndexHeader& indexHeader_, const uint8_t* key) {
    auto hash = keyHashFunc(key);
    auto slotId = hash & indexHeader_.levelHashMask;
    if (slotId < indexHeader_.nextSplitSlotId) {
        slotId = hash & indexHeader_.higherLevelHashMask;
    }
    return slotId;
}

template<typename T>
HashIndexBuilder<T>::HashIndexBuilder(const string& fName, const DataType& keyDataType)
    : BaseHashIndex{keyDataType}, numEntries{0} {
    fileHandle = make_unique<FileHandle>(fName, FileHandle::O_PERSISTENT_FILE_CREATE_NOT_EXISTS);
    indexHeader = make_unique<HashIndexHeader>(keyDataType.typeID);
    fileHandle->addNewPage(); // INDEX_HEADER_ARRAY_HEADER_PAGE
    fileHandle->addNewPage(); // P_SLOTS_HEADER_PAGE
    fileHandle->addNewPage(); // O_SLOTS_HEADER_PAGE
    headerArray = make_unique<InMemDiskArrayBuilder<HashIndexHeader>>(
        *fileHandle, INDEX_HEADER_ARRAY_HEADER_PAGE_IDX, 0 /* numElements */);
    pSlots = make_unique<InMemDiskArrayBuilder<Slot<T>>>(
        *fileHandle, P_SLOTS_HEADER_PAGE_IDX, 0 /* numElements */);
    // Reserve a slot for oSlots, which is always skipped, as we treat slot idx 0 as NULL.
    oSlots = make_unique<InMemDiskArrayBuilder<Slot<T>>>(
        *fileHandle, O_SLOTS_HEADER_PAGE_IDX, 1 /* numElements */);
    allocatePSlots(2);
    if (keyDataType.typeID == STRING) {
        inMemOverflowFile =
            make_unique<InMemOverflowFile>(StorageUtils::getOverflowFileName(fName));
    }
    keyInsertFunc = InMemHashIndexUtils::initializeInsertFunc(indexHeader->keyDataTypeID);
    keyEqualsFunc = InMemHashIndexUtils::initializeEqualsFunc(indexHeader->keyDataTypeID);
}

template<typename T>
void HashIndexBuilder<T>::bulkReserve(uint32_t numEntries_) {
    slot_id_t numRequiredEntries = getNumRequiredEntries(numEntries.load(), numEntries_);
    // Build from scratch.
    auto numRequiredSlots =
        (numRequiredEntries + HashIndexConfig::SLOT_CAPACITY - 1) / HashIndexConfig::SLOT_CAPACITY;
    auto numSlotsOfCurrentLevel = 1 << indexHeader->currentLevel;
    while ((numSlotsOfCurrentLevel << 1) < numRequiredSlots) {
        indexHeader->incrementLevel();
        numSlotsOfCurrentLevel = numSlotsOfCurrentLevel << 1;
    }
    if (numRequiredSlots > numSlotsOfCurrentLevel) {
        indexHeader->nextSplitSlotId = numRequiredSlots - numSlotsOfCurrentLevel;
    }
    allocatePSlots(numRequiredSlots);
}

template<typename T>
bool HashIndexBuilder<T>::appendInternal(const uint8_t* key, node_offset_t value) {
    SlotInfo pSlotInfo{getPrimarySlotIdForKey(*indexHeader, key), SlotType::PRIMARY};
    auto currentSlotInfo = pSlotInfo;
    Slot<T>* currentSlot = nullptr;
    lockSlot(pSlotInfo);
    while (currentSlotInfo.slotType == SlotType::PRIMARY || currentSlotInfo.slotId != 0) {
        currentSlot = getSlot(currentSlotInfo);
        if (lookupOrExistsInSlotWithoutLock<false /* exists */>(currentSlot, key)) {
            // Key already exists. No append is allowed.
            unlockSlot(pSlotInfo);
            return false;
        }
        if (currentSlot->header.numEntries < HashIndexConfig::SLOT_CAPACITY) {
            break;
        }
        currentSlotInfo.slotId = currentSlot->header.nextOvfSlotId;
        currentSlotInfo.slotType = SlotType::OVF;
    }
    assert(currentSlot);
    insertToSlotWithoutLock(currentSlot, key, value);
    unlockSlot(pSlotInfo);
    numEntries.fetch_add(1);
    return true;
}

template<typename T>
bool HashIndexBuilder<T>::lookupInternalWithoutLock(const uint8_t* key, node_offset_t& result) {
    SlotInfo pSlotInfo{getPrimarySlotIdForKey(*indexHeader, key), SlotType::PRIMARY};
    SlotInfo currentSlotInfo = pSlotInfo;
    Slot<T>* currentSlot;
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

template<typename T>
uint32_t HashIndexBuilder<T>::allocatePSlots(uint32_t numSlotsToAllocate) {
    unique_lock xLock{pSlotSharedMutex};
    auto oldNumSlots = pSlots->getNumElements();
    auto newNumSlots = oldNumSlots + numSlotsToAllocate;
    pSlots->resize(newNumSlots, true /* setToZero */);
    pSlotsMutexes.resize(newNumSlots);
    for (auto i = oldNumSlots; i < newNumSlots; i++) {
        pSlotsMutexes[i] = make_unique<mutex>();
    }
    return oldNumSlots;
}

template<typename T>
uint32_t HashIndexBuilder<T>::allocateAOSlot() {
    unique_lock xLock{oSlotsSharedMutex};
    auto oldNumSlots = oSlots->getNumElements();
    auto newNumSlots = oldNumSlots + 1;
    oSlots->resize(newNumSlots, true /* setToZero */);
    return oldNumSlots;
}

template<typename T>
Slot<T>* HashIndexBuilder<T>::getSlot(const SlotInfo& slotInfo) {
    if (slotInfo.slotType == SlotType::PRIMARY) {
        shared_lock sLck{pSlotSharedMutex};
        return &pSlots->operator[](slotInfo.slotId);
    } else {
        shared_lock sLck{oSlotsSharedMutex};
        return &oSlots->operator[](slotInfo.slotId);
    }
}

template<typename T>
template<bool IS_LOOKUP>
bool HashIndexBuilder<T>::lookupOrExistsInSlotWithoutLock(
    Slot<T>* slot, const uint8_t* key, node_offset_t* result) {
    for (auto entryPos = 0u; entryPos < HashIndexConfig::SLOT_CAPACITY; entryPos++) {
        if (!slot->header.isEntryValid(entryPos)) {
            continue;
        }
        auto& entry = slot->entries[entryPos];
        if (keyEqualsFunc(key, entry.data, inMemOverflowFile.get())) {
            if constexpr (IS_LOOKUP) {
                memcpy(result, entry.data + indexHeader->numBytesPerKey, sizeof(node_offset_t));
            }
            return true;
        }
    }
    return false;
}

template<typename T>
void HashIndexBuilder<T>::insertToSlotWithoutLock(
    Slot<T>* slot, const uint8_t* key, node_offset_t value) {
    if (slot->header.numEntries == HashIndexConfig::SLOT_CAPACITY) {
        // Allocate a new oSlot and change the nextOvfSlotId.
        auto ovfSlotId = allocateAOSlot();
        slot->header.nextOvfSlotId = ovfSlotId;
        slot = getSlot(SlotInfo{ovfSlotId, SlotType::OVF});
    }
    for (auto entryPos = 0u; entryPos < HashIndexConfig::SLOT_CAPACITY; entryPos++) {
        if (!slot->header.isEntryValid(entryPos)) {
            keyInsertFunc(key, value, slot->entries[entryPos].data, inMemOverflowFile.get());
            slot->header.setEntryValid(entryPos);
            slot->header.numEntries++;
            break;
        }
    }
}

template<typename T>
void HashIndexBuilder<T>::flush() {
    indexHeader->numEntries = numEntries.load();
    headerArray->resize(1, true /* setToZero */);
    headerArray->operator[](0) = *indexHeader;
    headerArray->saveToDisk();
    pSlots->saveToDisk();
    oSlots->saveToDisk();
    if (indexHeader->keyDataTypeID == STRING) {
        inMemOverflowFile->flush();
    }
}

template class HashIndexBuilder<int64_t>;
template class HashIndexBuilder<ku_string_t>;

} // namespace storage
} // namespace kuzu
