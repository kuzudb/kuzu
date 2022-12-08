#pragma once

#include <utility>

#include "function/hash/hash_operations.h"
#include "hash_index_builder.h"
#include "storage/storage_structure/disk_overflow_file.h"

namespace kuzu {
namespace storage {

enum class HashIndexLocalLookupState : uint8_t { KEY_FOUND, KEY_DELETED, KEY_NOT_EXIST };
enum class ChainedSlotsAction : uint8_t { LOOKUP_IN_SLOTS, DELETE_IN_SLOTS, FIND_FREE_SLOT };

template<typename T>
class TemplatedHashIndexLocalStorage {
public:
    HashIndexLocalLookupState lookup(const T& key, node_offset_t& result);
    void deleteKey(const T& key);
    bool insert(const T& key, node_offset_t value);

    inline bool hasUpdates() const { return !(localInsertions.empty() && localDeletions.empty()); }
    inline void clear() {
        localInsertions.clear();
        localDeletions.clear();
    }

    unordered_map<T, node_offset_t> localInsertions;
    unordered_set<T> localDeletions;
};

// Local storage consists of two in memory indexes. One (localInsertionIndex) is to keep track of
// all newly inserted entries, and the other (localDeletionIndex) is to keep track of newly deleted
// entries (not available in localInsertionIndex). We assume that in a transaction, the insertions
// and deletions are very small, thus they can be kept in memory.
class HashIndexLocalStorage {
public:
    explicit HashIndexLocalStorage(DataType keyDataType) : keyDataType{std::move(keyDataType)} {}
    // Currently, we assume that reads(lookup) and writes(delete/insert) of the local storage will
    // never happen concurrently. Thus, lookup requires no local storage lock. Writes are
    // coordinated to execute in serial with the help of the localStorageMutex. This is a
    // simplification to the lock scheme, but can be relaxed later if necessary.
    HashIndexLocalLookupState lookup(const uint8_t* key, node_offset_t& result);
    void deleteKey(const uint8_t* key);
    bool insert(const uint8_t* key, node_offset_t value);
    void applyLocalChanges(const std::function<void(const uint8_t*)>& deleteOp,
        const std::function<void(const uint8_t*, node_offset_t)>& insertOp);

    bool hasUpdates() const;
    void clear();

public:
    shared_mutex localStorageSharedMutex;

private:
    DataType keyDataType;
    TemplatedHashIndexLocalStorage<int64_t> templatedLocalStorageForInt;
    TemplatedHashIndexLocalStorage<string> templatedLocalStorageForString;
};

// HashIndex is the entrance to handle all updates and lookups into the index after building from
// scratch through InMemHashIndex.
// The index consists of two parts, one is the persistent storage (from the persistent index file),
// and the other is the local storage. All lookups/deletions/insertions go through local storage,
// and then the persistent storage if necessary.
//
// Key interfaces:
// - lookup(): Given a key, find its result. Return true if the key is found, else, return false.
//   Lookups go through the local storage first, check if the key is marked as deleted or not, then
//   check whether it can be found inside local insertions or not. If the key is neither marked as
//   deleted nor found in local insertions, we proceed to lookups in the persistent store.
// - delete(): Delete the given key.
//   Deletions are directly marked in the local storage.
// - insert(): Insert the given key and value. Return true if the given key doesn't exist in the
// index before insertion, otherwise, return false.
//   First check if the key to be inserted already exists in local insertions or the persistent
//   store. If the key doesn't exist yet, append it to local insertions, and also remove it from
//   local deletions if it was marked as deleted.
template<typename T>
class HashIndex : public BaseHashIndex {

public:
    HashIndex(const StorageStructureIDAndFName& storageStructureIDAndFName,
        const DataType& keyDataType, BufferManager& bufferManager, WAL* wal);

public:
    bool lookupInternal(Transaction* transaction, const uint8_t* key, node_offset_t& result);
    void deleteInternal(const uint8_t* key) const;
    bool insertInternal(const uint8_t* key, node_offset_t value);

    void prepareCommitOrRollbackIfNecessary(bool isCommit);
    void checkpointInMemoryIfNecessary();
    void rollbackInMemoryIfNecessary() const;
    inline VersionedFileHandle* getFileHandle() const { return fileHandle.get(); }

private:
    template<ChainedSlotsAction action>
    bool performActionInChainedSlots(TransactionType trxType, HashIndexHeader& header,
        SlotInfo& slotInfo, const uint8_t* key, node_offset_t& result);
    bool lookupInPersistentIndex(
        TransactionType trxType, const uint8_t* key, node_offset_t& result);
    // The following two functions are only used in prepareCommit, and are not thread-safe.
    void insertIntoPersistentIndex(const uint8_t* key, node_offset_t value);
    void deleteFromPersistentIndex(const uint8_t* key);

    void copyAndUpdateSlotHeader(bool isCopyEntry, Slot<T>& slot, entry_pos_t entryPos,
        const uint8_t* key, node_offset_t value);
    void copyKVOrEntryToSlot(bool isCopyEntry, const SlotInfo& slotInfo, Slot<T>& slot,
        const uint8_t* key, node_offset_t value);
    void splitSlot(HashIndexHeader& header);
    void rehashSlots(HashIndexHeader& header);
    vector<pair<SlotInfo, Slot<T>>> getChainedSlots(slot_id_t pSlotId);
    void copyEntryToSlot(slot_id_t slotId, uint8_t* entry);

    void prepareCommit();

    entry_pos_t findMatchedEntryInSlot(
        TransactionType trxType, const Slot<T>& slot, const uint8_t* key) const;

    void loopChainedSlotsToFindOneWithFreeSpace(SlotInfo& slotInfo, Slot<T>& slot);

    inline void updateSlot(const SlotInfo& slotInfo, Slot<T>& slot) const {
        slotInfo.slotType == SlotType::PRIMARY ? pSlots->update(slotInfo.slotId, slot) :
                                                 oSlots->update(slotInfo.slotId, slot);
    }
    inline Slot<T> getSlot(TransactionType trxType, const SlotInfo& slotInfo) const {
        return slotInfo.slotType == SlotType::PRIMARY ? pSlots->get(slotInfo.slotId, trxType) :
                                                        oSlots->get(slotInfo.slotId, trxType);
    }

public:
    StorageStructureIDAndFName storageStructureIDAndFName;
    BufferManager& bm;
    WAL* wal;
    unique_ptr<VersionedFileHandle> fileHandle;
    unique_ptr<BaseDiskArray<HashIndexHeader>> headerArray;
    unique_ptr<BaseDiskArray<Slot<T>>> pSlots;
    unique_ptr<BaseDiskArray<Slot<T>>> oSlots;
    insert_function_t keyInsertFunc;
    equals_function_t keyEqualsFunc;
    unique_ptr<DiskOverflowFile> diskOverflowFile;
    unique_ptr<HashIndexLocalStorage> localStorage;
};

class PrimaryKeyIndex {

    friend class HashIndexInt64Test;
    friend class HashIndexStringTest;

public:
    PrimaryKeyIndex(const StorageStructureIDAndFName& storageStructureIDAndFName,
        const DataType& keyDataType, BufferManager& bufferManager, WAL* wal)
        : keyDataTypeID{keyDataType.typeID} {
        if (keyDataTypeID == INT64) {
            hashIndexForInt64 = make_unique<HashIndex<int64_t>>(
                storageStructureIDAndFName, keyDataType, bufferManager, wal);
        } else {
            hashIndexForString = make_unique<HashIndex<ku_string_t>>(
                storageStructureIDAndFName, keyDataType, bufferManager, wal);
        }
    }

    bool lookup(
        Transaction* trx, ValueVector* keyVector, uint64_t vectorPos, node_offset_t& result);

    void deleteKey(ValueVector* keyVector, uint64_t vectorPos);

    bool insert(ValueVector* keyVector, uint64_t vectorPos, node_offset_t value);

    // These two lookups are used by InMemRelCSVCopier.
    inline bool lookup(Transaction* transaction, int64_t key, node_offset_t& result) {
        assert(keyDataTypeID == INT64);
        return hashIndexForInt64->lookupInternal(
            transaction, reinterpret_cast<const uint8_t*>(&key), result);
    }
    inline bool lookup(Transaction* transaction, const char* key, node_offset_t& result) {
        assert(keyDataTypeID == STRING);
        return hashIndexForString->lookupInternal(
            transaction, reinterpret_cast<const uint8_t*>(key), result);
    }

    inline void checkpointInMemoryIfNecessary() {
        keyDataTypeID == INT64 ? hashIndexForInt64->checkpointInMemoryIfNecessary() :
                                 hashIndexForString->checkpointInMemoryIfNecessary();
    }
    inline void rollbackInMemoryIfNecessary() {
        keyDataTypeID == INT64 ? hashIndexForInt64->rollbackInMemoryIfNecessary() :
                                 hashIndexForString->rollbackInMemoryIfNecessary();
    }
    inline void prepareCommitOrRollbackIfNecessary(bool isCommit) {
        return keyDataTypeID == INT64 ?
                   hashIndexForInt64->prepareCommitOrRollbackIfNecessary(isCommit) :
                   hashIndexForString->prepareCommitOrRollbackIfNecessary(isCommit);
    }
    inline VersionedFileHandle* getFileHandle() {
        return keyDataTypeID == INT64 ? hashIndexForInt64->getFileHandle() :
                                        hashIndexForString->getFileHandle();
    }
    inline DiskOverflowFile* getDiskOverflowFile() {
        return keyDataTypeID == STRING ? hashIndexForString->diskOverflowFile.get() : nullptr;
    }

private:
    inline void deleteKey(int64_t key) {
        assert(keyDataTypeID == INT64);
        hashIndexForInt64->deleteInternal(reinterpret_cast<const uint8_t*>(&key));
    }
    inline void deleteKey(const char* key) {
        assert(keyDataTypeID == STRING);
        hashIndexForString->deleteInternal(reinterpret_cast<const uint8_t*>(key));
    }
    inline bool insert(int64_t key, node_offset_t value) {
        assert(keyDataTypeID == INT64);
        return hashIndexForInt64->insertInternal(reinterpret_cast<const uint8_t*>(&key), value);
    }
    inline bool insert(const char* key, node_offset_t value) {
        assert(keyDataTypeID == STRING);
        return hashIndexForString->insertInternal(reinterpret_cast<const uint8_t*>(key), value);
    }

private:
    DataTypeID keyDataTypeID;
    unique_ptr<HashIndex<int64_t>> hashIndexForInt64;
    unique_ptr<HashIndex<ku_string_t>> hashIndexForString;
};

} // namespace storage
} // namespace kuzu
