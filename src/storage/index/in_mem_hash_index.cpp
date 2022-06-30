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

void SlotArray::addNewPagesWithoutLock(uint64_t numPages) {
    assert(file != nullptr);
    auto currentNumPages = pagesLogicalToPhysicalMapper.size();
    auto requiredNumPages = currentNumPages + numPages;
    pagesLogicalToPhysicalMapper.resize(requiredNumPages);
    for (auto i = currentNumPages; i < requiredNumPages; i++) {
        pagesLogicalToPhysicalMapper[i] = file->addANewPage(true);
    }
}

uint64_t SlotArray::allocateSlot() {
    unique_lock xLck{sharedGlobalMutex};
    if (nextSlotIdToAllocate == slotsCapacity) {
        // Require adding a new page, and extend the mutexes array accordingly.
        addNewPagesWithoutLock(1);
        slotsCapacity += numSlotsPerPage;
    }
    return nextSlotIdToAllocate++;
}

PageElementCursor SlotArray::getPageCursorForSlot(uint64_t slotId) {
    shared_lock sLck{sharedGlobalMutex};
    auto [logicalPageIdx, slotIdInPage] =
        StorageUtils::getQuotientRemainder(slotId, numSlotsPerPage);
    assert(logicalPageIdx < pagesLogicalToPhysicalMapper.size());
    auto physicalPageIdx = pagesLogicalToPhysicalMapper[logicalPageIdx];
    return PageElementCursor{physicalPageIdx, (uint16_t)slotIdInPage};
}

void SlotWithMutexArray::addNewPagesWithoutLock(uint64_t numPages) {
    SlotArray::addNewPagesWithoutLock(numPages);
    addSlotMutexesForPagesWithoutLock(numPages);
}

void SlotWithMutexArray::addSlotMutexesForPagesWithoutLock(uint64_t numPages) {
    auto currentNumMutexes = mutexes.size();
    assert(currentNumMutexes == slotsCapacity);
    auto requiredNumMutexes = currentNumMutexes + (numPages * numSlotsPerPage);
    mutexes.resize(requiredNumMutexes);
    for (auto i = currentNumMutexes; i < requiredNumMutexes; i++) {
        mutexes[i] = make_unique<mutex>();
    }
}

InMemHashIndexBuilder::InMemHashIndexBuilder(string fName, const DataType& keyDataType)
    : BaseHashIndex{keyDataType}, fName{move(fName)} {
    assert(keyDataType.typeID == INT64 || keyDataType.typeID == STRING);
    inMemFile = make_unique<InMemFile>(
        this->fName, 1, false, 1 /* initialize the hash index file with the header page. */);
    if (keyDataType.typeID == STRING) {
        inMemOverflowFile =
            make_unique<InMemOverflowFile>(StorageUtils::getOverflowPagesFName(this->fName));
    }
    indexHeader = make_unique<HashIndexHeader>(keyDataType.typeID);
    pSlots = make_unique<SlotWithMutexArray>(inMemFile.get(), indexHeader->numSlotsPerPage);
    oSlots = make_unique<SlotArray>(inMemFile.get(), indexHeader->numSlotsPerPage);
    // Initialize functions.
    keyInsertFunc = InMemHashIndexUtils::initializeInsertFunc(indexHeader->keyDataTypeID);
    keyEqualsFunc = InMemHashIndexUtils::initializeEqualsFunc(indexHeader->keyDataTypeID);
}

// bulkReserve is NOT thread-safe, and only safe to be performed only once when the index is empty.
void InMemHashIndexBuilder::bulkReserve(uint32_t numEntries_) {
    auto requiredNumEntries = (uint64_t)(numEntries_ * DEFAULT_HT_LOAD_FACTOR);
    auto requiredNumSlots =
        (requiredNumEntries + HashIndexConfig::SLOT_CAPACITY - 1) / HashIndexConfig::SLOT_CAPACITY;
    auto numSlotsOfCurrentLevel = 1 << indexHeader->currentLevel;
    while ((numSlotsOfCurrentLevel << 1) < requiredNumSlots) {
        indexHeader->incrementLevel();
        numSlotsOfCurrentLevel = numSlotsOfCurrentLevel << 1;
    }
    indexHeader->nextSplitSlotId = requiredNumSlots - numSlotsOfCurrentLevel;
    auto requiredNumPages =
        (requiredNumSlots + indexHeader->numSlotsPerPage - 1) / indexHeader->numSlotsPerPage;
    // Pre-allocate all pages. The first page has already been allocated and page 0 is reserved.
    pSlots->addNewPagesWithoutLock(requiredNumPages);
    pSlots->setNextSlotIdToAllocate(requiredNumSlots);
    // Pre-allocate a page for ovf slots, and also skip the first oSlot.
    oSlots->allocateSlot();
}

bool InMemHashIndexBuilder::appendInternal(const uint8_t* key, node_offset_t value) {
    SlotInfo pSlotInfo{getPrimarySlotIdForKey(key), true};
    SlotInfo prevSlotInfo{UINT64_MAX, true};
    auto currentSlotInfo = pSlotInfo;
    uint8_t* currentSlot = nullptr;
    lockSlot(pSlotInfo);
    while (currentSlotInfo.isPSlot || currentSlotInfo.slotId > 0) {
        currentSlot = getSlot(currentSlotInfo);
        if (existsInSlot(currentSlot, key)) {
            // Key already exists. No append is allowed.
            unlockSlot(pSlotInfo);
            return false;
        }
        prevSlotInfo = currentSlotInfo;
        currentSlotInfo.slotId = (reinterpret_cast<SlotHeader*>(currentSlot))->nextOvfSlotId;
        currentSlotInfo.isPSlot = false;
    }
    // At the end of the above while loop, currentSlotInfo is set to a garbage slot to break the
    // loop, and prev is the one we really need. Reset currentSlotInfo here.
    currentSlotInfo = prevSlotInfo;
    assert(currentSlot);
    if (reinterpret_cast<SlotHeader*>(currentSlot)->numEntries == HashIndexConfig::SLOT_CAPACITY) {
        // Allocate a new oSlot and change the nextOvfSlotId.
        currentSlotInfo.slotId = oSlots->allocateSlot();
        currentSlotInfo.isPSlot = false;
        reinterpret_cast<SlotHeader*>(currentSlot)->nextOvfSlotId = currentSlotInfo.slotId;
        currentSlot = getSlot(currentSlotInfo);
    }
    insertToSlot(currentSlot, key, value);
    unlockSlot(pSlotInfo);
    return true;
}

bool InMemHashIndexBuilder::existsInSlot(uint8_t* slot, const uint8_t* key) const {
    auto slotHeader = reinterpret_cast<SlotHeader*>(slot);
    for (auto entryPos = 0u; entryPos < slotHeader->numEntries; entryPos++) {
        if (slotHeader->isEntryDeleted(entryPos)) {
            continue;
        }
        auto entry = getEntryInSlot(const_cast<uint8_t*>(slot), entryPos);
        if (keyEqualsFunc(key, entry, inMemOverflowFile.get())) {
            return true;
        }
    }
    return false;
}

void InMemHashIndexBuilder::insertToSlot(uint8_t* slot, const uint8_t* key, node_offset_t value) {
    auto slotHeader = reinterpret_cast<SlotHeader*>(slot);
    auto entry = getEntryInSlot(slot, slotHeader->numEntries);
    keyInsertFunc(key, value, entry, inMemOverflowFile.get());
    slotHeader->numEntries++;
}

page_idx_t InMemHashIndexBuilder::writeLogicalToPhysicalMapper(SlotArray* slotsArray) {
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

void InMemHashIndexBuilder::flush() {
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
