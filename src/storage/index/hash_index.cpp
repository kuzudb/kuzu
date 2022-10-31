#include "include/hash_index.h"

#include "include/hash_index_utils.h"

#include "src/common/include/exception.h"

using namespace graphflow::common;

namespace graphflow {
namespace storage {

bool HashIndex::lookup(
    Transaction* trx, ValueVector* keyVector, uint64_t vectorPos, node_offset_t& result) {
    assert(!keyVector->isNull(vectorPos));
    switch (keyVector->dataType.typeID) {
    case INT64: {
        return lookup(trx, ((int64_t*)keyVector->values)[vectorPos], result);
    }
    default:
        throw RuntimeException("Index lookup on data type " +
                               Types::dataTypeToString(keyVector->dataType) +
                               " is not implemented.");
    }
}

void HashIndex::deleteKey(Transaction* trx, ValueVector* keyVector, uint64_t vectorPos) {
    assert(!keyVector->isNull(vectorPos));
    switch (keyVector->dataType.typeID) {
    case INT64: {
        deleteKey(trx, ((int64_t*)keyVector->values)[vectorPos]);
        return;
    }
    default:
        throw RuntimeException("Index delete on data type " +
                               Types::dataTypeToString(keyVector->dataType) +
                               " is not implemented.");
    }
}

bool HashIndex::insert(
    Transaction* trx, ValueVector* keyVector, uint64_t vectorPos, node_offset_t value) {
    assert(!keyVector->isNull(vectorPos));
    switch (keyVector->dataType.typeID) {
    case INT64: {
        auto key = ((int64_t*)keyVector->values)[vectorPos];
        if (!insert(trx, key, value)) {
            throw RuntimeException("Failed to insert primary key " + to_string(key));
        }
        return true;
    }
    default:
        throw RuntimeException("Index insert on data type " +
                               Types::dataTypeToString(keyVector->dataType) +
                               " is not implemented.");
    }
}

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
    : BaseHashIndex{keyDataType},
      storageStructureIDAndFName{storageStructureIDAndFName}, bm{bufferManager}, wal{wal} {
    assert(keyDataType.typeID == INT64);
    fileHandle = make_unique<VersionedFileHandle>(
        storageStructureIDAndFName, FileHandle::O_PERSISTENT_FILE_NO_CREATE);
    headerArray = make_unique<InMemDiskArray<HashIndexHeader>>(
        *fileHandle, INDEX_HEADER_ARRAY_HEADER_PAGE_IDX, &bm, wal);
    // Read indexHeader from the headerArray, which contains only one element.
    indexHeader =
        make_unique<HashIndexHeader>(headerArray->get(INDEX_HEADER_IDX_IN_ARRAY, READ_ONLY));
    assert(indexHeader->keyDataTypeID == keyDataType.typeID);
    pSlots = make_unique<InMemDiskArray<Slot>>(*fileHandle, P_SLOTS_HEADER_PAGE_IDX, &bm, wal);
    oSlots = make_unique<InMemDiskArray<Slot>>(*fileHandle, O_SLOTS_HEADER_PAGE_IDX, &bm, wal);
    // Initialize functions.
    keyHashFunc = HashIndexUtils::initializeHashFunc(indexHeader->keyDataTypeID);
    keyInsertFunc = HashIndexUtils::initializeInsertFunc(indexHeader->keyDataTypeID);
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

// For deletions, we don't check if the deleted keys exist or not. Thus, we don't need to check in
// the persistent storage and directly delete keys in the local storage.
void HashIndex::deleteInternal(Transaction* transaction, const uint8_t* key) const {
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
        if (lookupInPersistentIndex(transaction->getType(), key, tmpResult)) {
            return false;
        }
    }
    return localStorage->insert(key, value);
}

template<ChainedSlotsAction action>
bool HashIndex::performActionInChainedSlots(TransactionType trxType, HashIndexHeader& header,
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
            auto entryPos = findMatchedEntryInSlot(slot, key);
            if (entryPos != SlotHeader::INVALID_ENTRY_POS) {
                if constexpr (action == ChainedSlotsAction::LOOKUP_IN_SLOTS) {
                    result = *(node_offset_t*)(slot.entries[entryPos].data +
                                               Types::getDataTypeSize(indexHeader->keyDataTypeID));
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

bool HashIndex::lookupInPersistentIndex(
    TransactionType trxType, const uint8_t* key, node_offset_t& result) {
    auto header = trxType == TransactionType::READ_ONLY ?
                      *indexHeader :
                      headerArray->get(INDEX_HEADER_IDX_IN_ARRAY, TransactionType::WRITE);
    SlotInfo slotInfo{getPrimarySlotIdForKey(header, key), SlotType::PRIMARY};
    return performActionInChainedSlots<ChainedSlotsAction::LOOKUP_IN_SLOTS>(
        trxType, header, slotInfo, key, result);
}

void HashIndex::insertIntoPersistentIndex(const uint8_t* key, node_offset_t value) {
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

void HashIndex::deleteFromPersistentIndex(const uint8_t* key) {
    auto header = headerArray->get(INDEX_HEADER_IDX_IN_ARRAY, TransactionType::WRITE);
    SlotInfo slotInfo{getPrimarySlotIdForKey(header, key), SlotType::PRIMARY};
    node_offset_t result;
    performActionInChainedSlots<ChainedSlotsAction::DELETE_IN_SLOTS>(
        TransactionType::WRITE, header, slotInfo, key, result);
    headerArray->update(INDEX_HEADER_IDX_IN_ARRAY, header);
}

void HashIndex::loopChainedSlotsToFindOneWithFreeSpace(SlotInfo& slotInfo, Slot& slot) {
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

void HashIndex::splitSlot(HashIndexHeader& header) {
    pSlots->pushBack(Slot{});
    rehashSlots(header);
    header.incrementNextSplitSlotId();
}

void HashIndex::rehashSlots(HashIndexHeader& header) {
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
            auto newSlotId = keyHashFunc(key) & header.higherLevelHashMask;
            copyEntryToSlot(newSlotId, key);
        }
    }
}

void HashIndex::copyEntryToSlot(slot_id_t slotId, uint8_t* entry) {
    SlotInfo slotInfo{slotId, SlotType::PRIMARY};
    Slot slot;
    loopChainedSlotsToFindOneWithFreeSpace(slotInfo, slot);
    copyKVOrEntryToSlot(true /* copy entry */, slotInfo, slot, entry, UINT32_MAX);
    updateSlot(slotInfo, slot);
}

vector<pair<SlotInfo, Slot>> HashIndex::getChainedSlots(slot_id_t pSlotId) {
    vector<pair<SlotInfo, Slot>> slots;
    SlotInfo slotInfo{pSlotId, SlotType::PRIMARY};
    while (slotInfo.slotType == SlotType::PRIMARY || slotInfo.slotId != 0) {
        auto slot = getSlot(TransactionType::WRITE, slotInfo);
        slots.emplace_back(slotInfo, slot);
        slotInfo.slotId = slot.header.nextOvfSlotId;
        slotInfo.slotType = SlotType::OVF;
    }
    return slots;
}

void HashIndex::copyAndUpdateSlotHeader(
    bool isCopyEntry, Slot& slot, entry_pos_t entryPos, const uint8_t* key, node_offset_t value) {
    if (isCopyEntry) {
        memcpy(slot.entries[entryPos].data, key, indexHeader->numBytesPerEntry);
    } else {
        keyInsertFunc(key, value, slot.entries[entryPos].data, diskOverflowFile.get());
    }
    slot.header.setEntryValid(entryPos);
    slot.header.numEntries++;
}

void HashIndex::copyKVOrEntryToSlot(bool isCopyEntry, const SlotInfo& slotInfo, Slot& slot,
    const uint8_t* key, node_offset_t value) {
    if (slot.header.numEntries == HashIndexConfig::SLOT_CAPACITY) {
        // Allocate a new oSlot, insert the entry to the new oSlot, and update slot's
        // nextOvfSlotId.
        Slot newSlot;
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

entry_pos_t HashIndex::findMatchedEntryInSlot(const Slot& slot, const uint8_t* key) const {
    for (auto entryPos = 0u; entryPos < HashIndexConfig::SLOT_CAPACITY; entryPos++) {
        if (!slot.header.isEntryValid(entryPos)) {
            continue;
        }
        if (keyEqualsFunc(key, slot.entries[entryPos].data, diskOverflowFile.get())) {
            return entryPos;
        }
    }
    return SlotHeader::INVALID_ENTRY_POS;
}

void HashIndex::prepareCommit() {
    for (auto& [key, val] : localStorage->localDeletions) {
        deleteFromPersistentIndex((uint8_t*)&key);
    }
    for (auto& [key, val] : localStorage->localInsertions) {
        insertIntoPersistentIndex((uint8_t*)&key, val);
    }
}

void HashIndex::prepareRollback() {
    indexHeader = make_unique<HashIndexHeader>(
        headerArray->get(INDEX_HEADER_ARRAY_HEADER_PAGE_IDX, TransactionType::READ_ONLY));
}

void HashIndex::prepareCommitOrRollbackIfNecessary(bool isCommit) {
    unique_lock xlock{localStorage->localStorageSharedMutex};
    if (!localStorage->hasUpdates()) {
        return;
    }
    wal->addToUpdatedNodeTables(storageStructureIDAndFName.storageStructureID.nodeIndexID.tableID);
    isCommit ? prepareCommit() : prepareRollback();
}

void HashIndex::checkpointInMemoryIfNecessary() {
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

void HashIndex::rollbackInMemoryIfNecessary() const {
    if (!localStorage->hasUpdates()) {
        return;
    }
    headerArray->rollbackInMemoryIfNecessary();
    pSlots->rollbackInMemoryIfNecessary();
    oSlots->rollbackInMemoryIfNecessary();
    localStorage->clear();
}

} // namespace storage
} // namespace graphflow
