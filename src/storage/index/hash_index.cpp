#include "src/storage/index/include/hash_index.h"

namespace graphflow {
namespace storage {

// For lookups in the local storage, we first check if the key is marked deleted, then check if it
// can be found in the localInsertionIndex or not. If the key is neither deleted nor found, we
// return the state KEY_NOT_EXIST.
HashIndexLocalLookupState HashIndexLocalStorage::lookup(const uint8_t* key, node_offset_t& result) {
    shared_lock sLck{localStorageSharedMutex};
    if (localDeletionIndex->lookupInternalWithoutLock(key, result)) {
        return HashIndexLocalLookupState::KEY_DELETED;
    } else if (localInsertionIndex->lookupInternalWithoutLock(key, result)) {
        return HashIndexLocalLookupState::KEY_FOUND;
    } else {
        return HashIndexLocalLookupState::KEY_NOT_EXIST;
    }
}

void HashIndexLocalStorage::deleteKey(const uint8_t* key) {
    unique_lock xLck{localStorageSharedMutex};
    if (!localInsertionIndex->deleteInternal(key)) {
        localDeletionIndex->appendInternal(key, NODE_OFFSET_PLACE_HOLDER);
    }
}

bool HashIndexLocalStorage::insert(const uint8_t* key, node_offset_t value) {
    unique_lock xLck{localStorageSharedMutex};
    // Always delete the key to be inserted from localDeletionIndex first (if any).
    localDeletionIndex->deleteInternal(key);
    return localInsertionIndex->appendInternal(key, value);
}

HashIndex::HashIndex(const StorageStructureIDAndFName& storageStructureIDAndFName,
    const DataType& keyDataType, BufferManager& bufferManager, bool isInMemory)
    : BaseHashIndex{keyDataType}, storageStructureIDAndFName{storageStructureIDAndFName},
      isInMemory{isInMemory}, bm{bufferManager} {
    assert(keyDataType.typeID == INT64 || keyDataType.typeID == STRING);
    initializeFileAndHeader(keyDataType);
    // Initialize functions.
    keyHashFunc = HashIndexUtils::initializeHashFunc(indexHeader->keyDataTypeID);
    keyEqualsFunc = HashIndexUtils::initializeEqualsFunc(indexHeader->keyDataTypeID);
    localStorage = make_unique<HashIndexLocalStorage>(keyDataType);
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

// For read transactions, local storage is skipped, lookups are performed on the persistent storage.
// For write transactions, lookups are performed in the local storage first, then in the persistent
// storage if necessary. In details, there are three cases for the local storage lookup:
// - the key is found in the local storage, directly return true;
// - the key has been marked as deleted in the local storage, return false;
// - the key is neither deleted nor found in the local storage, lookup in the persistent storage.
bool HashIndex::lookupInternal(
    Transaction* transaction, const uint8_t* key, node_offset_t& result) {
    assert(localStorage && transaction);
    if (transaction->isReadOnly()) {
        return lookupInPersistentIndex(key, result);
    } else {
        assert(transaction->isWriteTransaction());
        auto localLookupState = localStorage->lookup(key, result);
        if (localLookupState == HashIndexLocalLookupState::KEY_FOUND) {
            return true;
        } else if (localLookupState == HashIndexLocalLookupState::KEY_DELETED) {
            return false;
        } else {
            assert(localLookupState == HashIndexLocalLookupState::KEY_NOT_EXIST);
            return lookupInPersistentIndex(key, result);
        }
    }
}

// For deletions, we don't check if the deleted keys exist or not. Thus, we don't need to check in
// the persistent storage and directly delete keys in the local storage.
void HashIndex::deleteInternal(Transaction* transaction, const uint8_t* key) {
    assert(localStorage && transaction && transaction->isWriteTransaction());
    localStorage->deleteKey(key);
}

// For insertions, we first check in the local storage. There are three cases:
// - the key is found in the local storage, return false;
// - the key is marked as deleted in the local storage, insert the key to the local storage;
// - the key doesn't exist in the local storage, check if the key exists in the persistent index, if
//   so, return false, else insert the key to the local storage.
bool HashIndex::insertInternal(Transaction* transaction, const uint8_t* key, node_offset_t value) {
    assert(localStorage && transaction && transaction->isWriteTransaction());
    node_offset_t tmpResult;
    auto localLookupState = localStorage->lookup(key, tmpResult);
    if (localLookupState == HashIndexLocalLookupState::KEY_FOUND) {
        return false;
    } else if (localLookupState == HashIndexLocalLookupState::KEY_NOT_EXIST) {
        if (lookupInPersistentIndex(key, tmpResult)) {
            return false;
        }
    }
    return localStorage->insert(key, value);
}

bool HashIndex::lookupInPersistentIndex(const uint8_t* key, node_offset_t& result) {
    SlotInfo prevSlotInfo{UINT64_MAX, true};
    SlotInfo pSlotInfo{getPrimarySlotIdForKey(key), true};
    SlotInfo currentSlotInfo = pSlotInfo;
    uint8_t* currentSlot = nullptr;
    while (currentSlotInfo.isPSlot || currentSlotInfo.slotId > 0) {
        currentSlot = pinSlot(currentSlotInfo);
        if (lookupInSlot(currentSlot, key, result)) {
            unpinSlot(currentSlotInfo);
            return true;
        }
        prevSlotInfo = currentSlotInfo;
        currentSlotInfo.slotId = reinterpret_cast<SlotHeader*>(currentSlot)->nextOvfSlotId;
        currentSlotInfo.isPSlot = false;
        unpinSlot(prevSlotInfo);
    }
    return false;
}

bool HashIndex::lookupInSlot(uint8_t* slot, const uint8_t* key, node_offset_t& result) const {
    auto slotHeader = reinterpret_cast<SlotHeader*>(slot);
    for (auto entryPos = 0u; entryPos < HashIndexConfig::SLOT_CAPACITY; entryPos++) {
        if (!slotHeader->isEntryValid(entryPos)) {
            continue;
        }
        auto entry = getEntryInSlot(const_cast<uint8_t*>(slot), entryPos);
        if (keyEqualsFunc(key, entry, overflowFile.get())) {
            memcpy(&result, entry + Types::getDataTypeSize(indexHeader->keyDataTypeID),
                sizeof(node_offset_t));
            return true;
        }
    }
    return false;
}

uint8_t* HashIndex::pinSlot(const SlotInfo& slotInfo) {
    auto pageCursor = slotInfo.isPSlot ? pSlots->getPageCursorForSlot(slotInfo.slotId) :
                                         oSlots->getPageCursorForSlot(slotInfo.slotId);
    auto frame = bm.pin(*fh, pageCursor.pageIdx);
    return frame + (pageCursor.elemPosInPage * indexHeader->numBytesPerSlot);
}

void HashIndex::unpinSlot(const SlotInfo& slotInfo) {
    auto pageCursor = slotInfo.isPSlot ? pSlots->getPageCursorForSlot(slotInfo.slotId) :
                                         oSlots->getPageCursorForSlot(slotInfo.slotId);
    bm.unpin(*fh, pageCursor.pageIdx);
}

} // namespace storage
} // namespace graphflow
