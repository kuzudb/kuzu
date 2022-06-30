#include "src/storage/index/include/hash_index.h"

namespace graphflow {
namespace storage {

HashIndex::HashIndex(const StorageStructureIDAndFName& storageStructureIDAndFName,
    const DataType& keyDataType, BufferManager& bufferManager, bool isInMemory)
    : BaseHashIndex{keyDataType}, storageStructureIDAndFName{storageStructureIDAndFName},
      isInMemory{isInMemory}, bm{bufferManager} {
    assert(keyDataType.typeID == INT64 || keyDataType.typeID == STRING);
    initializeFileAndHeader(keyDataType);
    // Initialize functions.
    keyHashFunc = HashIndexUtils::initializeHashFunc(indexHeader->keyDataTypeID);
    keyEqualsFunc = HashIndexUtils::initializeEqualsFunc(indexHeader->keyDataTypeID);
}

HashIndex::~HashIndex() {
    if (isInMemory) {
        StorageStructureUtils::unpinEachPageOfFile(*fh, bm);
    }
    bm.removeFilePagesFromFrames(*fh);
}

void HashIndex::initializeFileAndHeader(const DataType& keyDataType) {
    fh = make_unique<FileHandle>(
        storageStructureIDAndFName.fName, FileHandle::O_DefaultPagedExistingDBFileDoNotCreate);
    assert(fh->getNumPages() > 0);
    // Read the index header and page mappings from file.
    auto buffer = bm.pin(*fh, INDEX_HEADER_PAGE_ID);
    auto otherIndexHeader = *(HashIndexHeader*)buffer;
    indexHeader = make_unique<HashIndexHeader>(otherIndexHeader);
    pSlots = make_unique<SlotWithMutexArray>(nullptr, indexHeader->numSlotsPerPage);
    oSlots = make_unique<SlotArray>(nullptr, indexHeader->numSlotsPerPage);
    bm.unpin(*fh, INDEX_HEADER_PAGE_ID);
    assert(indexHeader->keyDataTypeID == keyDataType.typeID);
    readPageMapper(pSlots->pagesLogicalToPhysicalMapper, indexHeader->pPagesMapperFirstPageIdx);
    readPageMapper(oSlots->pagesLogicalToPhysicalMapper, indexHeader->oPagesMapperFirstPageIdx);
    pSlots->addSlotMutexesForPagesWithoutLock(pSlots->pagesLogicalToPhysicalMapper.size());
    if (isInMemory) {
        StorageStructureUtils::pinEachPageOfFile(*fh, bm);
    }
    if (indexHeader->keyDataTypeID == STRING) {
        overflowFile = make_unique<OverflowFile>(
            storageStructureIDAndFName, bm, isInMemory, nullptr /* no wal for now */);
    }
}

void HashIndex::readPageMapper(vector<page_idx_t>& mapper, page_idx_t mapperFirstPageIdx) {
    auto currentPageIdx = mapperFirstPageIdx;
    uint64_t elementPosInMapperToCopy = 0;
    while (currentPageIdx != UINT32_MAX) {
        auto frame = bm.pin(*fh, currentPageIdx);
        auto pageElements = (page_idx_t*)frame;
        auto numElementsInPage = pageElements[0];
        mapper.resize(elementPosInMapperToCopy + numElementsInPage);
        memcpy((uint8_t*)&mapper[elementPosInMapperToCopy], (uint8_t*)&pageElements[2],
            sizeof(page_idx_t) * numElementsInPage);
        auto nextPageIdx = pageElements[1];
        bm.unpin(*fh, currentPageIdx);
        elementPosInMapperToCopy += numElementsInPage;
        currentPageIdx = nextPageIdx;
    }
}

template<bool IS_DELETE>
bool HashIndex::lookupOrDeleteInSlot(
    uint8_t* slot, const uint8_t* key, node_offset_t* result) const {
    auto slotHeader = reinterpret_cast<SlotHeader*>(slot);
    for (auto entryPos = 0u; entryPos < slotHeader->numEntries; entryPos++) {
        if (slotHeader->isEntryDeleted(entryPos)) {
            continue;
        }
        auto entry = getEntryInSlot(const_cast<uint8_t*>(slot), entryPos);
        if (keyEqualsFunc(key, entry, overflowFile.get())) {
            if constexpr (IS_DELETE) {
                slotHeader->setEntryDeleted(entryPos);
            } else {
                memcpy(result, entry + Types::getDataTypeSize(indexHeader->keyDataTypeID),
                    sizeof(node_offset_t));
            }
            return true;
        }
    }
    return false;
}

// Lookups and Deletions follow the same pattern that they compute the initial slot, and traverse
// the chain to find the matched slot. Differences are that: 1) they perform different actions.
// Lookup copies the matched value, while deletion sets the mask for deleted entry in SlotHeader; 2)
// we assume lookups are not mixed with deletions/insertions, thus they can be performed without
// locks, while deletions require locks in the context of multi-threads.
template<bool IS_DELETE>
bool HashIndex::lookupOrDeleteInternal(const uint8_t* key, node_offset_t* result) {
    SlotInfo prevSlotInfo{UINT64_MAX, true};
    SlotInfo pSlotInfo{getPrimarySlotIdForKey(key), true};
    SlotInfo currentSlotInfo = pSlotInfo;
    if constexpr (IS_DELETE) {
        lockSlot(pSlotInfo);
    }
    uint8_t* currentSlot = nullptr;
    while (currentSlotInfo.isPSlot || currentSlotInfo.slotId > 0) {
        currentSlot = pinSlot(currentSlotInfo);
        if (lookupOrDeleteInSlot<IS_DELETE>(currentSlot, key, result)) {
            unpinSlot(currentSlotInfo);
            if constexpr (IS_DELETE) {
                unlockSlot(pSlotInfo);
            }
            return true;
        }
        prevSlotInfo = currentSlotInfo;
        currentSlotInfo.slotId = reinterpret_cast<SlotHeader*>(currentSlot)->nextOvfSlotId;
        currentSlotInfo.isPSlot = false;
        unpinSlot(prevSlotInfo);
    }
    if constexpr (IS_DELETE) {
        unlockSlot(pSlotInfo);
    }
    return false;
}

// Forward declarations for template functions in the cpp file.
template bool HashIndex::lookupOrDeleteInSlot<true>(
    uint8_t* slot, const uint8_t* key, node_offset_t* result) const;
template bool HashIndex::lookupOrDeleteInSlot<false>(
    uint8_t* slot, const uint8_t* key, node_offset_t* result) const;
template bool HashIndex::lookupOrDeleteInternal<true>(const uint8_t* key, node_offset_t* result);
template bool HashIndex::lookupOrDeleteInternal<false>(const uint8_t* key, node_offset_t* result);

uint8_t* HashIndex::pinSlot(const SlotInfo& slotInfo) {
    auto pageCursor = slotInfo.isPSlot ? pSlots->getPageCursorForSlot(slotInfo.slotId) :
                                         oSlots->getPageCursorForSlot(slotInfo.slotId);
    auto frame = bm.pin(*fh, pageCursor.pageIdx);
    return frame + (pageCursor.posInPage * indexHeader->numBytesPerSlot);
}

void HashIndex::unpinSlot(const SlotInfo& slotInfo) {
    auto pageCursor = slotInfo.isPSlot ? pSlots->getPageCursorForSlot(slotInfo.slotId) :
                                         oSlots->getPageCursorForSlot(slotInfo.slotId);
    bm.unpin(*fh, pageCursor.pageIdx);
}

void HashIndex::flush() {
    // Populate and flush the index header page.
    auto headerFrame = bm.pin(*fh, INDEX_HEADER_PAGE_ID);
    memcpy(headerFrame, (uint8_t*)indexHeader.get(), sizeof(HashIndexHeader));
    fh->writePage(headerFrame, INDEX_HEADER_PAGE_ID);
    bm.unpin(*fh, INDEX_HEADER_PAGE_ID);
    for (auto i = INDEX_HEADER_PAGE_ID; i < fh->getNumPages(); i++) {
        auto frame = bm.pin(*fh, i);
        fh->writePage(frame, i);
        bm.unpin(*fh, i);
    }
}

} // namespace storage
} // namespace graphflow
