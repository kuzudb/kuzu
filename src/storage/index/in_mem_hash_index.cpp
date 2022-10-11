#include "src/storage/index/include/in_mem_hash_index.h"

namespace graphflow {
namespace storage {

HashIndexHeader::HashIndexHeader(DataTypeID keyDataTypeID) : keyDataTypeID{keyDataTypeID} {
    numBytesPerEntry = Types::getDataTypeSize(this->keyDataTypeID) + sizeof(node_offset_t);
    numBytesPerSlot =
        (numBytesPerEntry << HashIndexConfig::SLOT_CAPACITY_LOG_2) + sizeof(SlotHeader);
    assert(numBytesPerSlot < DEFAULT_PAGE_SIZE);
    numSlotsPerPage = DEFAULT_PAGE_SIZE / numBytesPerSlot;
}

void HashIndexHeader::incrementNextSplitSlotId() {
    if (nextSplitSlotId < (1ull << currentLevel) - 1) {
        nextSplitSlotId++;
    } else {
        incrementLevel();
    }
}

void SlotArray::addNewPagesWithoutLock(uint64_t numPages) {
    assert(file != nullptr);
    auto currentNumPages = pagesLogicalToPhysicalMapper.size();
    auto requiredNumPages = currentNumPages + numPages;
    pagesLogicalToPhysicalMapper.resize(requiredNumPages);
    for (auto i = currentNumPages; i < requiredNumPages; i++) {
        pagesLogicalToPhysicalMapper[i] = file->addANewPage(true);
    }
}

slot_id_t SlotArray::allocateSlotWithoutLock() {
    if (nextSlotIdToAllocate % numSlotsPerPage == 0) {
        // Require adding a new page, and extend the mutexes array accordingly.
        addNewPagesWithoutLock(1);
    }
    return nextSlotIdToAllocate++;
}

PageElementCursor SlotArray::getPageCursorForSlotWithoutLock(slot_id_t slotId) {
    auto [logicalPageIdx, slotIdInPage] =
        StorageUtils::getQuotientRemainder(slotId, numSlotsPerPage);
    assert(logicalPageIdx < pagesLogicalToPhysicalMapper.size());
    auto physicalPageIdx = pagesLogicalToPhysicalMapper[logicalPageIdx];
    return PageElementCursor{physicalPageIdx, (uint16_t)slotIdInPage};
}

void SlotWithMutexArray::addSlotMutexesForPagesWithoutLock(uint64_t numPages) {
    auto currentNumMutexes = mutexes.size();
    auto requiredNumMutexes = currentNumMutexes + (numPages * numSlotsPerPage);
    mutexes.resize(requiredNumMutexes);
    for (auto i = currentNumMutexes; i < requiredNumMutexes; i++) {
        mutexes[i] = make_unique<mutex>();
    }
}

slot_id_t BaseHashIndex::getPrimarySlotIdForKey(const uint8_t* key) {
    auto hash = keyHashFunc(key);
    auto slotId = hash & indexHeader->levelHashMask;
    if (slotId < indexHeader->nextSplitSlotId) {
        slotId = hash & indexHeader->higherLevelHashMask;
    }
    return slotId;
}

InMemHashIndex::InMemHashIndex(string fName, const DataType& keyDataType)
    : BaseHashIndex{keyDataType}, fName{move(fName)} {
    assert(keyDataType.typeID == INT64 || keyDataType.typeID == STRING);
    inMemFile = make_unique<InMemFile>(this->fName, 1 /* numBytesForElement */, false,
        1 /* initialize the hash index file with the header page. */);
    if (keyDataType.typeID == STRING) {
        inMemOverflowFile =
            make_unique<InMemOverflowFile>(StorageUtils::getOverflowFileName(this->fName));
    }
    indexHeader = make_unique<HashIndexHeader>(keyDataType.typeID);
    pSlots = make_unique<SlotWithMutexArray>(inMemFile.get(), indexHeader->numSlotsPerPage);
    oSlots = make_unique<SlotArray>(inMemFile.get(), indexHeader->numSlotsPerPage);
    pSlots->addNewPagesWithoutLock(1);
    pSlots->setNextSlotIdToAllocate(2);
    // Pre-allocate a page for ovf slots, and also skip the first oSlot.
    oSlots->allocateSlot();
    // Initialize functions.
    keyInsertFunc = InMemHashIndexUtils::initializeInsertFunc(indexHeader->keyDataTypeID);
    keyEqualsFunc = InMemHashIndexUtils::initializeEqualsFunc(indexHeader->keyDataTypeID);
}

void InMemHashIndex::bulkReserve(uint32_t numEntries_) {
    unique_lock lck{pSlots->sharedGlobalMutex};
    slot_id_t numRequiredEntries = ceil((numEntries.load() + numEntries_) * DEFAULT_HT_LOAD_FACTOR);
    // Build from scratch.
    auto numRequiredSlots =
        (numRequiredEntries + HashIndexConfig::SLOT_CAPACITY - 1) / HashIndexConfig::SLOT_CAPACITY;
    auto numSlotsOfCurrentLevel = 1 << indexHeader->currentLevel;
    while ((numSlotsOfCurrentLevel << 1) < numRequiredSlots) {
        indexHeader->incrementLevel();
        numSlotsOfCurrentLevel = numSlotsOfCurrentLevel << 1;
    }
    indexHeader->nextSplitSlotId = numRequiredSlots - numSlotsOfCurrentLevel;
    auto numRequiredPages =
        (numRequiredSlots + indexHeader->numSlotsPerPage - 1) / indexHeader->numSlotsPerPage;
    // Pre-allocate all pages. The first page has already been allocated and page 0 is reserved.
    pSlots->addNewPagesWithoutLock(numRequiredPages);
    pSlots->setNextSlotIdToAllocate(numRequiredSlots);
}

bool InMemHashIndex::appendInternal(const uint8_t* key, node_offset_t value) {
    slot_id_t numRequiredEntries = ceil((numEntries.load() + 1) * DEFAULT_HT_LOAD_FACTOR);
    while (numRequiredEntries > pSlots->getNumAllocatedSlots() * HashIndexConfig::SLOT_CAPACITY) {
        splitSlotWithoutLock();
    }
    SlotInfo pSlotInfo{getPrimarySlotIdForKey(key), true};
    auto currentSlotInfo = pSlotInfo;
    uint8_t* currentSlot = nullptr;
    lockSlot(pSlotInfo);
    while (currentSlotInfo.isPSlot || currentSlotInfo.slotId > 0) {
        currentSlot = getSlot(currentSlotInfo);
        if (lookupOrExistsInSlotWithoutLock<false /* exists */>(currentSlot, key)) {
            // Key already exists. No append is allowed.
            unlockSlot(pSlotInfo);
            return false;
        }
        if (((SlotHeader*)currentSlot)->numEntries < HashIndexConfig::SLOT_CAPACITY) {
            break;
        }
        currentSlotInfo.slotId = (reinterpret_cast<SlotHeader*>(currentSlot))->nextOvfSlotId;
        currentSlotInfo.isPSlot = false;
    }
    // At the end of the above while loop, currentSlotInfo is set to a garbage slot to break the
    // loop, and prev is the one we really need. Reset currentSlotInfo here.
    assert(currentSlot);
    insertToSlotWithoutLock(currentSlot, key, value);
    unlockSlot(pSlotInfo);
    numEntries.fetch_add(1);
    return true;
}

bool InMemHashIndex::deleteInternal(const uint8_t* key) {
    SlotInfo nextSlotInfo{getPrimarySlotIdForKey(key), true};
    auto pSlotInfo = nextSlotInfo;
    lockSlot(pSlotInfo);
    uint8_t* slot;
    while (nextSlotInfo.isPSlot || nextSlotInfo.slotId > 0) {
        slot = getSlot(nextSlotInfo);
        auto entryPos = findMatchedEntryInSlotWithoutLock(slot, key);
        if (entryPos != SlotHeader::INVALID_ENTRY_POS) {
            auto slotHeader = reinterpret_cast<SlotHeader*>(slot);
            slotHeader->setEntryInvalid(entryPos);
            slotHeader->numEntries--;
            unlockSlot(pSlotInfo);
            numEntries.fetch_sub(1);
            return true;
        }
        nextSlotInfo.slotId = ((SlotHeader*)slot)->nextOvfSlotId;
        nextSlotInfo.isPSlot = false;
    }
    unlockSlot(pSlotInfo);
    return false;
}

bool InMemHashIndex::lookupInternalWithoutLock(const uint8_t* key, node_offset_t& result) {
    SlotInfo pSlotInfo{getPrimarySlotIdForKey(key), true};
    SlotInfo currentSlotInfo = pSlotInfo;
    uint8_t* currentSlot = nullptr;
    while (currentSlotInfo.isPSlot || currentSlotInfo.slotId > 0) {
        currentSlot = getSlot(currentSlotInfo);
        if (lookupOrExistsInSlotWithoutLock<true /* lookup */>(currentSlot, key, &result)) {
            return true;
        }
        currentSlotInfo.slotId = reinterpret_cast<SlotHeader*>(currentSlot)->nextOvfSlotId;
        currentSlotInfo.isPSlot = false;
    }
    return false;
}

template<bool IS_LOOKUP>
bool InMemHashIndex::lookupOrExistsInSlotWithoutLock(
    uint8_t* slot, const uint8_t* key, node_offset_t* result) {
    auto slotHeader = reinterpret_cast<SlotHeader*>(slot);
    for (auto entryPos = 0u; entryPos < HashIndexConfig::SLOT_CAPACITY; entryPos++) {
        if (!slotHeader->isEntryValid(entryPos)) {
            continue;
        }
        auto entry = getEntryInSlot(const_cast<uint8_t*>(slot), entryPos);
        if (keyEqualsFunc(key, entry, inMemOverflowFile.get())) {
            if constexpr (IS_LOOKUP) {
                memcpy(result, entry + Types::getDataTypeSize(indexHeader->keyDataTypeID),
                    sizeof(node_offset_t));
            }
            return true;
        }
    }
    return false;
}

void InMemHashIndex::insertToSlotWithoutLock(
    uint8_t* slot, const uint8_t* key, node_offset_t value) {
    auto slotHeader = reinterpret_cast<SlotHeader*>(slot);
    if (slotHeader->numEntries == HashIndexConfig::SLOT_CAPACITY) {
        // Allocate a new oSlot and change the nextOvfSlotId.
        auto ovfSlotId = oSlots->allocateSlot();
        slotHeader->nextOvfSlotId = ovfSlotId;
        slot = getSlot(SlotInfo{ovfSlotId, false});
        slotHeader = reinterpret_cast<SlotHeader*>(slot);
    }
    for (auto entryPos = 0u; entryPos < HashIndexConfig::SLOT_CAPACITY; entryPos++) {
        if (!slotHeader->isEntryValid(entryPos)) {
            keyInsertFunc(key, value, getEntryInSlot(slot, entryPos), inMemOverflowFile.get());
            slotHeader->setEntryValid(entryPos);
            slotHeader->numEntries++;
            break;
        }
    }
}

void InMemHashIndex::splitSlotWithoutLock() {
    pSlots->allocateSlotWithoutLock();
    rehashSlotsWithoutLock();
    indexHeader->incrementNextSplitSlotId();
}

void InMemHashIndex::rehashSlotsWithoutLock() {
    auto slotsToSplit = getPAndOSlotsWithoutLock(indexHeader->nextSplitSlotId);
    for (auto slot : slotsToSplit) {
        auto slotHeader = *(SlotHeader*)slot;
        ((SlotHeader*)slot)->reset();
        for (auto entryPos = 0u; entryPos < HashIndexConfig::SLOT_CAPACITY; entryPos++) {
            if (!slotHeader.isEntryValid(entryPos)) {
                continue; // Skip deleted entries.
            }
            auto key = getEntryInSlot(slot, entryPos);
            // TODO(Guodong): fix the rehash for gfStrings.
            auto newSlotId = keyHashFunc(key) & indexHeader->higherLevelHashMask;
            copyEntryToSlotWithoutLock(newSlotId, key);
        }
    }
}

void InMemHashIndex::copyEntryToSlotWithoutLock(uint64_t slotId, uint8_t* entry) {
    SlotInfo slotInfo{slotId, true};
    uint8_t* slot;
    while (slotInfo.isPSlot || slotInfo.slotId > 0) {
        slot = getSlot(slotInfo);
        slotInfo.slotId = ((SlotHeader*)slot)->nextOvfSlotId;
        slotInfo.isPSlot = false;
        if (((SlotHeader*)slot)->numEntries != HashIndexConfig::SLOT_CAPACITY) {
            // Found a slot with empty space.
            break;
        }
    }
    auto slotHeader = (SlotHeader*)slot;
    if (slotHeader->numEntries == HashIndexConfig::SLOT_CAPACITY) {
        auto newOvfSlotId = oSlots->allocateSlotWithoutLock();
        ((SlotHeader*)slot)->nextOvfSlotId = newOvfSlotId;
        slot = getSlot(SlotInfo{newOvfSlotId, false});
        slotHeader = (SlotHeader*)slot;
    }
    for (auto entryPos = 0u; entryPos < HashIndexConfig::SLOT_CAPACITY; entryPos++) {
        if (!slotHeader->isEntryValid(entryPos)) {
            memcpy(getEntryInSlot(slot, entryPos), entry, indexHeader->numBytesPerEntry);
            slotHeader->setEntryValid(entryPos);
            slotHeader->numEntries++;
            break;
        }
    }
}

vector<uint8_t*> InMemHashIndex::getPAndOSlotsWithoutLock(uint64_t pSlotId) {
    vector<uint8_t*> slots;
    SlotInfo nextSlotInfo{pSlotId, true};
    while (nextSlotInfo.isPSlot || nextSlotInfo.slotId != 0) {
        auto slot = getSlot(nextSlotInfo);
        slots.push_back(slot);
        nextSlotInfo.slotId = ((SlotHeader*)slot)->nextOvfSlotId;
        nextSlotInfo.isPSlot = false;
    }
    return slots;
}

entry_pos_t InMemHashIndex::findMatchedEntryInSlotWithoutLock(uint8_t* slot, const uint8_t* key) {
    auto slotHeader = reinterpret_cast<SlotHeader*>(slot);
    for (auto entryPos = 0u; entryPos < HashIndexConfig::SLOT_CAPACITY; entryPos++) {
        if (!slotHeader->isEntryValid(entryPos)) {
            continue;
        }
        auto entry = getEntryInSlot(const_cast<uint8_t*>(slot), entryPos);
        if (keyEqualsFunc(key, entry, inMemOverflowFile.get())) {
            return entryPos;
        }
    }
    return SlotHeader::INVALID_ENTRY_POS;
}

page_idx_t InMemHashIndex::writeLogicalToPhysicalMapper(SlotArray* slotsArray) {
    if (slotsArray->pagesLogicalToPhysicalMapper.empty()) {
        return UINT32_MAX;
    }
    auto slotsMapperFirstPageIdx = inMemFile->getNumPages();
    // In each page, the first uint32_t indicates num of elements, the next one is nextPageIdx.
    auto maxNumElementsInAPage = DEFAULT_PAGE_SIZE / sizeof(page_idx_t) - 2;
    auto remainingElements = (uint64_t)slotsArray->pagesLogicalToPhysicalMapper.size();
    uint64_t elementPosInMapperToCopy = 0;
    while (remainingElements > 0) {
        auto currentPageIdx = inMemFile->addANewPage(false);
        auto numElementsInCurrentPage = min(remainingElements, maxNumElementsInAPage);
        auto pageElements = (page_idx_t*)inMemFile->getPage(currentPageIdx)->data;
        pageElements[0] = numElementsInCurrentPage;
        pageElements[1] =
            remainingElements > numElementsInCurrentPage ? (currentPageIdx + 1) : UINT32_MAX;
        assert(elementPosInMapperToCopy + numElementsInCurrentPage <=
               slotsArray->pagesLogicalToPhysicalMapper.size());
        memcpy((uint8_t*)&pageElements[2],
            (uint8_t*)&slotsArray->pagesLogicalToPhysicalMapper[elementPosInMapperToCopy],
            sizeof(page_idx_t) * numElementsInCurrentPage);
        remainingElements -= numElementsInCurrentPage;
        elementPosInMapperToCopy += numElementsInCurrentPage;
    }
    return slotsMapperFirstPageIdx;
}

void InMemHashIndex::flush() {
    assert(!fName.empty());
    // Copy index header back to the page.
    indexHeader->pPagesMapperFirstPageIdx = writeLogicalToPhysicalMapper(pSlots.get());
    indexHeader->oPagesMapperFirstPageIdx = writeLogicalToPhysicalMapper(oSlots.get());
    // Populate and flush the index header page.
    memcpy(inMemFile->getPage(INDEX_HEADER_PAGE_ID)->data, (uint8_t*)indexHeader.get(),
        sizeof(HashIndexHeader));
    // Flush files.
    inMemFile->flush();
    if (inMemOverflowFile) {
        inMemOverflowFile->flush();
    }
}

} // namespace storage
} // namespace graphflow
