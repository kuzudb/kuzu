#include "include/hash_index.h"

#include "include/hash_index_utils.h"

namespace graphflow {
namespace storage {

HashIndexLocalLookupState HashIndexLocalStorage::lookup(const uint8_t* key, node_offset_t& result) {
    shared_lock sLck{localStorageSharedMutex};
    auto keyVal = *(int64_t*)key;
    if (localDeletions.contains(keyVal)) {
        result = localDeletions[keyVal];
        return HashIndexLocalLookupState::KEY_DELETED;
    } else if (localInsertions.contains(keyVal)) {
        result = localInsertions[keyVal];
        return HashIndexLocalLookupState::KEY_FOUND;
    } else {
        return HashIndexLocalLookupState::KEY_NOT_EXIST;
    }
}

void HashIndexLocalStorage::deleteKey(const uint8_t* key) {
    unique_lock xLck{localStorageSharedMutex};
    auto keyVal = *(int64_t*)key;
    if (localInsertions.contains(keyVal)) {
        localInsertions.erase(keyVal);
    } else {
        localDeletions[keyVal] = NODE_OFFSET_PLACE_HOLDER;
    }
}

bool HashIndexLocalStorage::insert(const uint8_t* key, node_offset_t value) {
    unique_lock xLck{localStorageSharedMutex};
    auto keyVal = *(int64_t*)key;
    if (localDeletions.contains(keyVal)) {
        localDeletions.erase(keyVal);
    }
    if (localInsertions.contains(keyVal)) {
        return false;
    }
    localInsertions[keyVal] = value;
    return true;
}

HashIndex::HashIndex(const StorageStructureIDAndFName& storageStructureIDAndFName,
    const DataType& keyDataType, BufferManager& bufferManager, WAL* wal)
    : BaseHashIndex{keyDataType}, bm{bufferManager} {
    assert(keyDataType.typeID == INT64);
    fileHandle = make_unique<VersionedFileHandle>(
        storageStructureIDAndFName, FileHandle::O_PERSISTENT_FILE_NO_CREATE);
    headerArray = make_unique<InMemDiskArray<HashIndexHeader>>(
        *fileHandle, INDEX_HEADER_ARRAY_HEADER_PAGE_IDX, &bm, wal);
    // Read indexHeader from the headerArray, which contains only one element.
    indexHeader = make_unique<HashIndexHeader>(headerArray->get(0));
    assert(indexHeader->keyDataTypeID == keyDataType.typeID);
    pSlots = make_unique<InMemDiskArray<Slot>>(*fileHandle, P_SLOTS_HEADER_PAGE_IDX, &bm, wal);
    oSlots = make_unique<InMemDiskArray<Slot>>(*fileHandle, O_SLOTS_HEADER_PAGE_IDX, &bm, wal);
    pSlotsMutexes.resize(pSlots->getNumElements());
    // Initialize functions.
    keyHashFunc = HashIndexUtils::initializeHashFunc(indexHeader->keyDataTypeID);
    keyEqualsFunc = HashIndexUtils::initializeEqualsFunc(indexHeader->keyDataTypeID);
    localStorage = make_unique<HashIndexLocalStorage>(keyDataType);
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
    SlotInfo pSlotInfo{getPrimarySlotIdForKey(key), true};
    SlotInfo currentSlotInfo = pSlotInfo;
    Slot* currentSlot;
    while (currentSlotInfo.isPSlot || currentSlotInfo.slotId > 0) {
        currentSlot = getSlot(currentSlotInfo);
        if (lookupInSlot(currentSlot, key, result)) {
            return true;
        }
        currentSlotInfo.slotId = reinterpret_cast<SlotHeader*>(currentSlot)->nextOvfSlotId;
        currentSlotInfo.isPSlot = false;
    }
    return false;
}

bool HashIndex::lookupInSlot(Slot* slot, const uint8_t* key, node_offset_t& result) const {
    for (auto entryPos = 0u; entryPos < HashIndexConfig::SLOT_CAPACITY; entryPos++) {
        if (!slot->header.isEntryValid(entryPos)) {
            continue;
        }
        auto entry = slot->entries[entryPos].data;
        if (keyEqualsFunc(key, entry, diskOverflowFile.get())) {
            memcpy(&result, entry + Types::getDataTypeSize(indexHeader->keyDataTypeID),
                sizeof(node_offset_t));
            return true;
        }
    }
    return false;
}

} // namespace storage
} // namespace graphflow
