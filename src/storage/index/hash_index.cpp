#include "storage/index/hash_index.h"

#include "common/exception.h"
#include "storage/index/hash_index_utils.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

template<typename T>
HashIndexLocalLookupState TemplatedHashIndexLocalStorage<T>::lookup(
    const T& key, node_offset_t& result) {
    if (localDeletions.contains(key)) {
        return HashIndexLocalLookupState::KEY_DELETED;
    } else if (localInsertions.contains(key)) {
        result = localInsertions[key];
        return HashIndexLocalLookupState::KEY_FOUND;
    } else {
        return HashIndexLocalLookupState::KEY_NOT_EXIST;
    }
}

template<typename T>
void TemplatedHashIndexLocalStorage<T>::deleteKey(const T& key) {
    if (localInsertions.contains(key)) {
        localInsertions.erase(key);
    } else {
        localDeletions.insert(key);
    }
}

template<typename T>
bool TemplatedHashIndexLocalStorage<T>::insert(const T& key, node_offset_t value) {
    if (localDeletions.contains(key)) {
        localDeletions.erase(key);
    }
    if (localInsertions.contains(key)) {
        return false;
    }
    localInsertions[key] = value;
    return true;
}

template class TemplatedHashIndexLocalStorage<int64_t>;
template class TemplatedHashIndexLocalStorage<string>;

HashIndexLocalLookupState HashIndexLocalStorage::lookup(const uint8_t* key, node_offset_t& result) {
    shared_lock sLck{localStorageSharedMutex};
    if (keyDataType.typeID == INT64) {
        auto keyVal = *(int64_t*)key;
        return templatedLocalStorageForInt.lookup(keyVal, result);
    } else {
        assert(keyDataType.typeID == STRING);
        auto keyVal = string((char*)key);
        return templatedLocalStorageForString.lookup(keyVal, result);
    }
}

void HashIndexLocalStorage::deleteKey(const uint8_t* key) {
    unique_lock xLck{localStorageSharedMutex};
    if (keyDataType.typeID == INT64) {
        auto keyVal = *(int64_t*)key;
        templatedLocalStorageForInt.deleteKey(keyVal);
    } else {
        assert(keyDataType.typeID == STRING);
        auto keyVal = string((char*)key);
        templatedLocalStorageForString.deleteKey(keyVal);
    }
}

bool HashIndexLocalStorage::insert(const uint8_t* key, node_offset_t value) {
    unique_lock xLck{localStorageSharedMutex};
    if (keyDataType.typeID == INT64) {
        auto keyVal = *(int64_t*)key;
        return templatedLocalStorageForInt.insert(keyVal, value);
    } else {
        assert(keyDataType.typeID == STRING);
        auto keyVal = string((char*)key);
        return templatedLocalStorageForString.insert(keyVal, value);
    }
}

void HashIndexLocalStorage::applyLocalChanges(const std::function<void(const uint8_t*)>& deleteOp,
    const std::function<void(const uint8_t*, node_offset_t)>& insertOp) {
    if (keyDataType.typeID == INT64) {
        for (auto& key : templatedLocalStorageForInt.localDeletions) {
            deleteOp((uint8_t*)&key);
        }
        for (auto& [key, val] : templatedLocalStorageForInt.localInsertions) {
            insertOp((uint8_t*)&key, val);
        }
    } else {
        assert(keyDataType.typeID == STRING);
        for (auto& key : templatedLocalStorageForString.localDeletions) {
            deleteOp((uint8_t*)key.c_str());
        }
        for (auto& [key, val] : templatedLocalStorageForString.localInsertions) {
            insertOp((uint8_t*)key.c_str(), val);
        }
    }
}

bool HashIndexLocalStorage::hasUpdates() const {
    if (keyDataType.typeID == INT64) {
        return templatedLocalStorageForInt.hasUpdates();
    } else {
        assert(keyDataType.typeID == STRING);
        return templatedLocalStorageForString.hasUpdates();
    }
}

void HashIndexLocalStorage::clear() {
    if (keyDataType.typeID == INT64) {
        templatedLocalStorageForInt.clear();
    } else {
        assert(keyDataType.typeID == STRING);
        templatedLocalStorageForString.clear();
    }
}

template<typename T>
HashIndex<T>::HashIndex(const StorageStructureIDAndFName& storageStructureIDAndFName,
    const DataType& keyDataType, BufferManager& bufferManager, WAL* wal)
    : BaseHashIndex{keyDataType},
      storageStructureIDAndFName{storageStructureIDAndFName}, bm{bufferManager}, wal{wal} {
    fileHandle = make_unique<VersionedFileHandle>(
        storageStructureIDAndFName, FileHandle::O_PERSISTENT_FILE_NO_CREATE);
    headerArray = make_unique<BaseDiskArray<HashIndexHeader>>(
        *fileHandle, INDEX_HEADER_ARRAY_HEADER_PAGE_IDX, &bm, wal);
    // Read indexHeader from the headerArray, which contains only one element.
    indexHeader =
        make_unique<HashIndexHeader>(headerArray->get(INDEX_HEADER_IDX_IN_ARRAY, READ_ONLY));
    assert(indexHeader->keyDataTypeID == keyDataType.typeID);
    pSlots = make_unique<BaseDiskArray<Slot<T>>>(*fileHandle, P_SLOTS_HEADER_PAGE_IDX, &bm, wal);
    oSlots = make_unique<BaseDiskArray<Slot<T>>>(*fileHandle, O_SLOTS_HEADER_PAGE_IDX, &bm, wal);
    // Initialize functions.
    keyHashFunc = HashIndexUtils::initializeHashFunc(indexHeader->keyDataTypeID);
    keyInsertFunc = HashIndexUtils::initializeInsertFunc(indexHeader->keyDataTypeID);
    keyEqualsFunc = HashIndexUtils::initializeEqualsFunc(indexHeader->keyDataTypeID);
    localStorage = make_unique<HashIndexLocalStorage>(keyDataType);
    if (keyDataType.typeID == STRING) {
        diskOverflowFile = make_unique<DiskOverflowFile>(
            storageStructureIDAndFName, bm, false /* on disk by default */, wal);
    }
}

// For read transactions, local storage is skipped, lookups are performed on the persistent
// storage. For write transactions, lookups are performed in the local storage first, then in
// the persistent storage if necessary. In details, there are three cases for the local storage
// lookup:
// - the key is found in the local storage, directly return true;
// - the key has been marked as deleted in the local storage, return false;
// - the key is neither deleted nor found in the local storage, lookup in the persistent
// storage.
template<typename T>
bool HashIndex<T>::lookupInternal(
    Transaction* transaction, const uint8_t* key, node_offset_t& result) {
    if (transaction->isReadOnly()) {
        return lookupInPersistentIndex(transaction->getType(), key, result);
    } else {
        assert(transaction->isWriteTransaction());
        auto localLookupState = localStorage->lookup(key, result);
        if (localLookupState == HashIndexLocalLookupState::KEY_FOUND) {
            return true;
        } else if (localLookupState == HashIndexLocalLookupState::KEY_DELETED) {
            return false;
        } else {
            assert(localLookupState == HashIndexLocalLookupState::KEY_NOT_EXIST);
            return lookupInPersistentIndex(transaction->getType(), key, result);
        }
    }
}

// For deletions, we don't check if the deleted keys exist or not. Thus, we don't need to check
// in the persistent storage and directly delete keys in the local storage.
template<typename T>
void HashIndex<T>::deleteInternal(const uint8_t* key) const {
    localStorage->deleteKey(key);
}

// For insertions, we first check in the local storage. There are three cases:
// - the key is found in the local storage, return false;
// - the key is marked as deleted in the local storage, insert the key to the local storage;
// - the key doesn't exist in the local storage, check if the key exists in the persistent
// index, if
//   so, return false, else insert the key to the local storage.
template<typename T>
bool HashIndex<T>::insertInternal(const uint8_t* key, node_offset_t value) {
    node_offset_t tmpResult;
    auto localLookupState = localStorage->lookup(key, tmpResult);
    if (localLookupState == HashIndexLocalLookupState::KEY_FOUND) {
        return false;
    } else if (localLookupState == HashIndexLocalLookupState::KEY_NOT_EXIST) {
        if (lookupInPersistentIndex(TransactionType::WRITE, key, tmpResult)) {
            return false;
        }
    }
    return localStorage->insert(key, value);
}

template<typename T>
template<ChainedSlotsAction action>
bool HashIndex<T>::performActionInChainedSlots(TransactionType trxType, HashIndexHeader& header,
    SlotInfo& slotInfo, const uint8_t* key, node_offset_t& result) {
    while (slotInfo.slotType == SlotType::PRIMARY || slotInfo.slotId != 0) {
        auto slot = getSlot(trxType, slotInfo);
        if constexpr (action == ChainedSlotsAction::FIND_FREE_SLOT) {
            if (slot.header.numEntries < HashIndexConfig::SLOT_CAPACITY ||
                slot.header.nextOvfSlotId == 0) {
                // Found a slot with empty space.
                break;
            }
        } else {
            auto entryPos = findMatchedEntryInSlot(trxType, slot, key);
            if (entryPos != SlotHeader::INVALID_ENTRY_POS) {
                if constexpr (action == ChainedSlotsAction::LOOKUP_IN_SLOTS) {
                    result = *(
                        node_offset_t*)(slot.entries[entryPos].data + indexHeader->numBytesPerKey);
                } else if constexpr (action == ChainedSlotsAction::DELETE_IN_SLOTS) {
                    slot.header.setEntryInvalid(entryPos);
                    slot.header.numEntries--;
                    updateSlot(slotInfo, slot);
                    header.numEntries--;
                }
                return true;
            }
        }
        slotInfo.slotId = slot.header.nextOvfSlotId;
        slotInfo.slotType = SlotType::OVF;
    }
    return false;
}

template<typename T>
bool HashIndex<T>::lookupInPersistentIndex(
    TransactionType trxType, const uint8_t* key, node_offset_t& result) {
    auto header = trxType == TransactionType::READ_ONLY ?
                      *indexHeader :
                      headerArray->get(INDEX_HEADER_IDX_IN_ARRAY, TransactionType::WRITE);
    SlotInfo slotInfo{getPrimarySlotIdForKey(header, key), SlotType::PRIMARY};
    return performActionInChainedSlots<ChainedSlotsAction::LOOKUP_IN_SLOTS>(
        trxType, header, slotInfo, key, result);
}

template<typename T>
void HashIndex<T>::insertIntoPersistentIndex(const uint8_t* key, node_offset_t value) {
    auto header = headerArray->get(INDEX_HEADER_IDX_IN_ARRAY, TransactionType::WRITE);
    slot_id_t numRequiredEntries = getNumRequiredEntries(header.numEntries, 1);
    while (numRequiredEntries >
           pSlots->getNumElements(TransactionType::WRITE) * HashIndexConfig::SLOT_CAPACITY) {
        splitSlot(header);
    }
    auto pSlotId = getPrimarySlotIdForKey(header, key);
    SlotInfo slotInfo{pSlotId, SlotType::PRIMARY};
    node_offset_t result;
    performActionInChainedSlots<ChainedSlotsAction::FIND_FREE_SLOT>(
        TransactionType::WRITE, header, slotInfo, key, result);
    Slot slot = getSlot(TransactionType::WRITE, slotInfo);
    copyKVOrEntryToSlot(false /* insert kv */, slotInfo, slot, key, value);
    header.numEntries++;
    headerArray->update(INDEX_HEADER_IDX_IN_ARRAY, header);
}

template<typename T>
void HashIndex<T>::deleteFromPersistentIndex(const uint8_t* key) {
    auto header = headerArray->get(INDEX_HEADER_IDX_IN_ARRAY, TransactionType::WRITE);
    SlotInfo slotInfo{getPrimarySlotIdForKey(header, key), SlotType::PRIMARY};
    node_offset_t result;
    performActionInChainedSlots<ChainedSlotsAction::DELETE_IN_SLOTS>(
        TransactionType::WRITE, header, slotInfo, key, result);
    headerArray->update(INDEX_HEADER_IDX_IN_ARRAY, header);
}

template<typename T>
void HashIndex<T>::loopChainedSlotsToFindOneWithFreeSpace(SlotInfo& slotInfo, Slot<T>& slot) {
    while (slotInfo.slotType == SlotType::PRIMARY || slotInfo.slotId > 0) {
        slot = getSlot(TransactionType::WRITE, slotInfo);
        if (slot.header.numEntries < HashIndexConfig::SLOT_CAPACITY ||
            slot.header.nextOvfSlotId == 0) {
            // Found a slot with empty space.
            break;
        }
        slotInfo.slotId = slot.header.nextOvfSlotId;
        slotInfo.slotType = SlotType::OVF;
    }
}

template<typename T>
void HashIndex<T>::splitSlot(HashIndexHeader& header) {
    pSlots->pushBack(Slot<T>{});
    rehashSlots(header);
    header.incrementNextSplitSlotId();
}

template<typename T>
void HashIndex<T>::rehashSlots(HashIndexHeader& header) {
    auto slotsToSplit = getChainedSlots(header.nextSplitSlotId);
    for (auto& [slotInfo, slot] : slotsToSplit) {
        auto slotHeader = slot.header;
        slot.header.reset();
        updateSlot(slotInfo, slot);
        for (auto entryPos = 0u; entryPos < HashIndexConfig::SLOT_CAPACITY; entryPos++) {
            if (!slotHeader.isEntryValid(entryPos)) {
                continue; // Skip invalid entries.
            }
            auto key = slot.entries[entryPos].data;
            hash_t hash;
            if (header.keyDataTypeID == STRING) {
                auto str = diskOverflowFile->readString(TransactionType::WRITE, *(ku_string_t*)key);
                hash = keyHashFunc((const uint8_t*)str.c_str());
            } else {
                hash = keyHashFunc(key);
            }
            auto newSlotId = hash & header.higherLevelHashMask;
            copyEntryToSlot(newSlotId, key);
        }
    }
}

template<typename T>
void HashIndex<T>::copyEntryToSlot(slot_id_t slotId, uint8_t* entry) {
    SlotInfo slotInfo{slotId, SlotType::PRIMARY};
    Slot<T> slot;
    loopChainedSlotsToFindOneWithFreeSpace(slotInfo, slot);
    copyKVOrEntryToSlot(true /* copy entry */, slotInfo, slot, entry, UINT32_MAX);
    updateSlot(slotInfo, slot);
}

template<typename T>
vector<pair<SlotInfo, Slot<T>>> HashIndex<T>::getChainedSlots(slot_id_t pSlotId) {
    vector<pair<SlotInfo, Slot<T>>> slots;
    SlotInfo slotInfo{pSlotId, SlotType::PRIMARY};
    while (slotInfo.slotType == SlotType::PRIMARY || slotInfo.slotId != 0) {
        auto slot = getSlot(TransactionType::WRITE, slotInfo);
        slots.emplace_back(slotInfo, slot);
        slotInfo.slotId = slot.header.nextOvfSlotId;
        slotInfo.slotType = SlotType::OVF;
    }
    return slots;
}

template<typename T>
void HashIndex<T>::copyAndUpdateSlotHeader(bool isCopyEntry, Slot<T>& slot, entry_pos_t entryPos,
    const uint8_t* key, node_offset_t value) {
    if (isCopyEntry) {
        memcpy(slot.entries[entryPos].data, key, indexHeader->numBytesPerEntry);
    } else {
        keyInsertFunc(key, value, slot.entries[entryPos].data, diskOverflowFile.get());
    }
    slot.header.setEntryValid(entryPos);
    slot.header.numEntries++;
}

template<typename T>
void HashIndex<T>::copyKVOrEntryToSlot(bool isCopyEntry, const SlotInfo& slotInfo, Slot<T>& slot,
    const uint8_t* key, node_offset_t value) {
    if (slot.header.numEntries == HashIndexConfig::SLOT_CAPACITY) {
        // Allocate a new oSlot, insert the entry to the new oSlot, and update slot's
        // nextOvfSlotId.
        Slot<T> newSlot;
        auto entryPos = 0u; // Always insert to the first entry when there is a new slot.
        copyAndUpdateSlotHeader(isCopyEntry, newSlot, entryPos, key, value);
        slot.header.nextOvfSlotId = oSlots->pushBack(newSlot);
    } else {
        for (auto entryPos = 0u; entryPos < HashIndexConfig::SLOT_CAPACITY; entryPos++) {
            if (!slot.header.isEntryValid(entryPos)) {
                copyAndUpdateSlotHeader(isCopyEntry, slot, entryPos, key, value);
                break;
            }
        }
    }
    updateSlot(slotInfo, slot);
}

template<typename T>
entry_pos_t HashIndex<T>::findMatchedEntryInSlot(
    TransactionType trxType, const Slot<T>& slot, const uint8_t* key) const {
    for (auto entryPos = 0u; entryPos < HashIndexConfig::SLOT_CAPACITY; entryPos++) {
        if (!slot.header.isEntryValid(entryPos)) {
            continue;
        }
        if (keyEqualsFunc(trxType, key, slot.entries[entryPos].data, diskOverflowFile.get())) {
            return entryPos;
        }
    }
    return SlotHeader::INVALID_ENTRY_POS;
}

template<typename T>
void HashIndex<T>::prepareCommit() {
    localStorage->applyLocalChanges(
        [this](const uint8_t* key) -> void { this->deleteFromPersistentIndex(key); },
        [this](const uint8_t* key, node_offset_t value) -> void {
            this->insertIntoPersistentIndex(key, value);
        });
}

template<typename T>
void HashIndex<T>::prepareCommitOrRollbackIfNecessary(bool isCommit) {
    unique_lock xlock{localStorage->localStorageSharedMutex};
    if (!localStorage->hasUpdates()) {
        return;
    }
    wal->addToUpdatedNodeTables(storageStructureIDAndFName.storageStructureID.nodeIndexID.tableID);
    if (isCommit) {
        prepareCommit();
    }
}

template<typename T>
void HashIndex<T>::checkpointInMemoryIfNecessary() {
    if (!localStorage->hasUpdates()) {
        return;
    }
    indexHeader = make_unique<HashIndexHeader>(
        headerArray->get(INDEX_HEADER_IDX_IN_ARRAY, TransactionType::WRITE));
    headerArray->checkpointInMemoryIfNecessary();
    pSlots->checkpointInMemoryIfNecessary();
    oSlots->checkpointInMemoryIfNecessary();
    localStorage->clear();
}

template<typename T>
void HashIndex<T>::rollbackInMemoryIfNecessary() const {
    if (!localStorage->hasUpdates()) {
        return;
    }
    headerArray->rollbackInMemoryIfNecessary();
    pSlots->rollbackInMemoryIfNecessary();
    oSlots->rollbackInMemoryIfNecessary();
    localStorage->clear();
}

template class HashIndex<int64_t>;
template class HashIndex<ku_string_t>;

bool PrimaryKeyIndex::lookup(
    Transaction* trx, ValueVector* keyVector, uint64_t vectorPos, node_offset_t& result) {
    assert(!keyVector->isNull(vectorPos));
    if (keyDataTypeID == INT64) {
        auto key = keyVector->getValue<int64_t>(vectorPos);
        return hashIndexForInt64->lookupInternal(
            trx, reinterpret_cast<const uint8_t*>(&key), result);
    } else {
        auto key = keyVector->getValue<ku_string_t>(vectorPos).getAsString();
        return hashIndexForString->lookupInternal(
            trx, reinterpret_cast<const uint8_t*>(key.c_str()), result);
    }
}

void PrimaryKeyIndex::deleteKey(ValueVector* keyVector, uint64_t vectorPos) {
    assert(!keyVector->isNull(vectorPos));
    if (keyDataTypeID == INT64) {
        auto key = keyVector->getValue<int64_t>(vectorPos);
        hashIndexForInt64->deleteInternal(reinterpret_cast<const uint8_t*>(&key));
    } else {
        auto key = keyVector->getValue<ku_string_t>(vectorPos).getAsString();
        hashIndexForString->deleteInternal(reinterpret_cast<const uint8_t*>(key.c_str()));
    }
}

bool PrimaryKeyIndex::insert(ValueVector* keyVector, uint64_t vectorPos, node_offset_t value) {
    assert(!keyVector->isNull(vectorPos));
    if (keyDataTypeID == INT64) {
        auto key = keyVector->getValue<int64_t>(vectorPos);
        return hashIndexForInt64->insertInternal(reinterpret_cast<const uint8_t*>(&key), value);
    } else {
        auto key = keyVector->getValue<ku_string_t>(vectorPos).getAsString();
        return hashIndexForString->insertInternal(
            reinterpret_cast<const uint8_t*>(key.c_str()), value);
    }
}

} // namespace storage
} // namespace kuzu
