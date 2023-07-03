#include "storage/index/hash_index_builder.h"

using namespace kuzu::common;

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
HashIndexBuilder<T>::HashIndexBuilder(const std::string& fName, const LogicalType& keyDataType)
    : BaseHashIndex{keyDataType}, numEntries{0} {
    fileHandle =
        std::make_unique<FileHandle>(fName, FileHandle::O_PERSISTENT_FILE_CREATE_NOT_EXISTS);
    indexHeader = std::make_unique<HashIndexHeader>(keyDataType.getLogicalTypeID());
    fileHandle->addNewPage(); // INDEX_HEADER_ARRAY_HEADER_PAGE
    fileHandle->addNewPage(); // P_SLOTS_HEADER_PAGE
    fileHandle->addNewPage(); // O_SLOTS_HEADER_PAGE
    headerArray = std::make_unique<InMemDiskArrayBuilder<HashIndexHeader>>(
        *fileHandle, INDEX_HEADER_ARRAY_HEADER_PAGE_IDX, 0 /* numElements */);
    pSlots = std::make_unique<InMemDiskArrayBuilder<Slot<T>>>(
        *fileHandle, P_SLOTS_HEADER_PAGE_IDX, 0 /* numElements */);
    // Reserve a slot for oSlots, which is always skipped, as we treat slot idx 0 as NULL.
    oSlots = std::make_unique<InMemDiskArrayBuilder<Slot<T>>>(
        *fileHandle, O_SLOTS_HEADER_PAGE_IDX, 1 /* numElements */);
    allocatePSlots(2);
    if (keyDataType.getLogicalTypeID() == LogicalTypeID::STRING) {
        inMemOverflowFile =
            std::make_unique<InMemOverflowFile>(StorageUtils::getOverflowFileName(fName));
    }
    keyInsertFunc = InMemHashIndexUtils::initializeInsertFunc(indexHeader->keyDataTypeID);
    keyEqualsFunc = InMemHashIndexUtils::initializeEqualsFunc(indexHeader->keyDataTypeID);
}

template<typename T>
void HashIndexBuilder<T>::bulkReserve(uint32_t numEntries_) {
    auto prevLevel = indexHeader->currentLevel;
    auto prevSplitSlotId = indexHeader->nextSplitSlotId;
    auto prevNumOfSlots = pSlots->getNumElements();

    slot_id_t numRequiredEntries = getNumRequiredEntries(numEntries.load(), numEntries_);
    // Build from scratch.
    auto numRequiredSlots = (numRequiredEntries + HashIndexConstants::SLOT_CAPACITY - 1) /
                            HashIndexConstants::SLOT_CAPACITY;
    auto numSlotsOfCurrentLevel = 1 << indexHeader->currentLevel;
    while ((numSlotsOfCurrentLevel << 1) < numRequiredSlots) {
        indexHeader->incrementLevel();
        numSlotsOfCurrentLevel = numSlotsOfCurrentLevel << 1;
    }
    if (numRequiredSlots > numSlotsOfCurrentLevel) {
        indexHeader->nextSplitSlotId = numRequiredSlots - numSlotsOfCurrentLevel;
    }
    allocatePSlots(numRequiredSlots);

    // Perform rehashing
    if (prevLevel != indexHeader->currentLevel) {
        for (int i = 0; i < prevNumOfSlots; i++) {
            rehashSlots(i);
        }
    } else {
        for (auto i = prevSplitSlotId; i < indexHeader->nextSplitSlotId; i++) {
            rehashSlots(i);
        }
    }
}

template<typename T>
bool HashIndexBuilder<T>::appendInternal(const uint8_t* key, offset_t value) {
    SlotInfo pSlotInfo{getPrimarySlotIdForKey(*indexHeader, key), SlotType::PRIMARY};
    auto currentSlotInfo = pSlotInfo;
    Slot<T>* currentSlot = nullptr;
    while (currentSlotInfo.slotType == SlotType::PRIMARY || currentSlotInfo.slotId != 0) {
        currentSlot = getSlot(currentSlotInfo);
        if (lookupOrExistsInSlotWithoutLock<false /* exists */>(currentSlot, key)) {
            // Key already exists. No append is allowed.
            return false;
        }
        if (currentSlot->header.numEntries < HashIndexConstants::SLOT_CAPACITY) {
            break;
        }
        currentSlotInfo.slotId = currentSlot->header.nextOvfSlotId;
        currentSlotInfo.slotType = SlotType::OVF;
    }
    assert(currentSlot);
    insertToSlotWithoutLock(currentSlot, key, value);
    numEntries.fetch_add(1);
    return true;
}

template<typename T>
void HashIndexBuilder<T>::bulkReserveIfRequired(uint32_t numEntries_) {
    auto totalEntries = pSlots->getNumElements() * HashIndexConstants::SLOT_CAPACITY;
    auto loadFactor = (double_t)(numEntries + numEntries_) / totalEntries;
    if (loadFactor > HashIndexConstants::MAX_LOAD_FACTOR) {
        bulkReserve(numEntries_);
    }
}

template<typename T>
bool HashIndexBuilder<T>::lookupInternalWithoutLock(const uint8_t* key, offset_t& result) {
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
void HashIndexBuilder<T>::rehashSlots(slot_id_t primarySlotId) {
    std::vector<std::pair<SlotInfo, Slot<T>>> slots;
    SlotInfo slotInfo{primarySlotId, SlotType::PRIMARY};
    while (slotInfo.slotType == SlotType::PRIMARY || slotInfo.slotId != 0) {
        auto slot = getSlot(slotInfo);
        slots.emplace_back(slotInfo, *slot);
        slotInfo.slotId = slot->header.nextOvfSlotId;
        slotInfo.slotType = SlotType::OVF;
    }

    for (auto& [sI, slot] : slots) {
        auto slotHeader = slot.header;
        slot.header.reset();
        for (auto entryPos = 0u; entryPos < HashIndexConstants::SLOT_CAPACITY; entryPos++) {
            if (!slotHeader.isEntryValid(entryPos)) {
                continue; // Skip invalid entries.
            }
            auto key = slot.entries[entryPos].data;
            slot_id_t newSlotId;
            if (indexHeader->keyDataTypeID == LogicalTypeID::STRING) {
                auto str = inMemOverflowFile->readString((ku_string_t*)key);
                newSlotId = getPrimarySlotIdForKey(*indexHeader, (const uint8_t*)str.c_str());
            } else {
                newSlotId = getPrimarySlotIdForKey(*indexHeader, key);
            }
            // copy key to newSlotId
            copyEntryToSlot(newSlotId, key);
        }
    }
}

template<typename T>
void HashIndexBuilder<T>::copyEntryToSlot(slot_id_t slotId, uint8_t* entry) {
    SlotInfo currentSlotInfo{slotId, SlotType::PRIMARY};
    Slot<T>* currentSlot = nullptr;
    while (currentSlotInfo.slotType == SlotType::PRIMARY || currentSlotInfo.slotId != 0) {
        currentSlot = getSlot(currentSlotInfo);
        if (currentSlot->header.numEntries < HashIndexConstants::SLOT_CAPACITY) {
            break;
        }
        currentSlotInfo.slotId = currentSlot->header.nextOvfSlotId;
        currentSlotInfo.slotType = SlotType::OVF;
    }

    if (currentSlot->header.numEntries == HashIndexConstants::SLOT_CAPACITY) {
        // Allocate a new oSlot and change the nextOvfSlotId.
        auto ovfSlotId = allocateAOSlot();
        currentSlot->header.nextOvfSlotId = ovfSlotId;
        currentSlot = getSlot(SlotInfo{ovfSlotId, SlotType::OVF});
    }

    for (auto entryPos = 0u; entryPos < HashIndexConstants::SLOT_CAPACITY; entryPos++) {
        if (!currentSlot->header.isEntryValid(entryPos)) {
            memcpy(currentSlot->entries[entryPos].data, entry, indexHeader->numBytesPerEntry);
            currentSlot->header.setEntryValid(entryPos);
            currentSlot->header.numEntries++;
            break;
        }
    }
}

template<typename T>
uint32_t HashIndexBuilder<T>::allocatePSlots(uint32_t numSlotsToAllocate) {
    auto oldNumSlots = pSlots->getNumElements();
    auto newNumSlots = oldNumSlots + numSlotsToAllocate;
    pSlots->resize(newNumSlots, true /* setToZero */);
    return oldNumSlots;
}

template<typename T>
uint32_t HashIndexBuilder<T>::allocateAOSlot() {
    auto oldNumSlots = oSlots->getNumElements();
    auto newNumSlots = oldNumSlots + 1;
    oSlots->resize(newNumSlots, true /* setToZero */);
    return oldNumSlots;
}

template<typename T>
Slot<T>* HashIndexBuilder<T>::getSlot(const SlotInfo& slotInfo) {
    if (slotInfo.slotType == SlotType::PRIMARY) {
        return &pSlots->operator[](slotInfo.slotId);
    } else {
        return &oSlots->operator[](slotInfo.slotId);
    }
}

template<typename T>
template<bool IS_LOOKUP>
bool HashIndexBuilder<T>::lookupOrExistsInSlotWithoutLock(
    Slot<T>* slot, const uint8_t* key, offset_t* result) {
    for (auto entryPos = 0u; entryPos < HashIndexConstants::SLOT_CAPACITY; entryPos++) {
        if (!slot->header.isEntryValid(entryPos)) {
            continue;
        }
        auto& entry = slot->entries[entryPos];
        if (keyEqualsFunc(key, entry.data, inMemOverflowFile.get())) {
            if constexpr (IS_LOOKUP) {
                memcpy(result, entry.data + indexHeader->numBytesPerKey, sizeof(offset_t));
            }
            return true;
        }
    }
    return false;
}

template<typename T>
void HashIndexBuilder<T>::insertToSlotWithoutLock(
    Slot<T>* slot, const uint8_t* key, offset_t value) {
    if (slot->header.numEntries == HashIndexConstants::SLOT_CAPACITY) {
        // Allocate a new oSlot and change the nextOvfSlotId.
        auto ovfSlotId = allocateAOSlot();
        slot->header.nextOvfSlotId = ovfSlotId;
        slot = getSlot(SlotInfo{ovfSlotId, SlotType::OVF});
    }
    for (auto entryPos = 0u; entryPos < HashIndexConstants::SLOT_CAPACITY; entryPos++) {
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
    if (indexHeader->keyDataTypeID == LogicalTypeID::STRING) {
        inMemOverflowFile->flush();
    }
}

template class HashIndexBuilder<int64_t>;
template class HashIndexBuilder<ku_string_t>;

} // namespace storage
} // namespace kuzu
