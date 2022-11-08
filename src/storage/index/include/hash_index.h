#pragma once

#include "hash_index_builder.h"

#include "src/function/hash/operations/include/hash_operations.h"
#include "src/storage/storage_structure/include/disk_overflow_file.h"

using namespace graphflow::common;

namespace graphflow {
namespace storage {

enum class HashIndexLocalLookupState : uint8_t { KEY_FOUND, KEY_DELETED, KEY_NOT_EXIST };
enum class ChainedSlotsAction : uint8_t { LOOKUP_IN_SLOTS, DELETE_IN_SLOTS, FIND_FREE_SLOT };

// Local storage consists of two in memory indexes. One (localInsertionIndex) is to keep track of
// all newly inserted entries, and the other (localDeletionIndex) is to keep track of newly deleted
// entries (not available in localInsertionIndex). We assume that in a transaction, the insertions
// and deletions are very small, thus they can be kept in memory.
// TODO(Guodong): Add the support of string keys.
class HashIndexLocalStorage {
    static const node_offset_t NODE_OFFSET_PLACE_HOLDER = UINT64_MAX;

public:
    explicit HashIndexLocalStorage(const DataType& keyDataType) {}

    // Currently, we assume that reads(lookup) and writes(delete/insert) of the local storage will
    // never happen concurrently. Thus, lookup requires no local storage lock. Writes are
    // coordinated to execute in serial with the help of the localStorageMutex. This is a
    // simplification to the lock scheme, but can be relaxed later if necessary.
    HashIndexLocalLookupState lookup(const uint8_t* key, node_offset_t& result);
    void deleteKey(const uint8_t* key);
    bool insert(const uint8_t* key, node_offset_t value);

    inline bool hasUpdates() const { return !(localInsertions.empty() && localDeletions.empty()); };
    inline void clear() {
        localInsertions.clear();
        localDeletions.clear();
    }

public:
    shared_mutex localStorageSharedMutex;
    unordered_map<int64_t, node_offset_t> localInsertions;
    unordered_map<int64_t, node_offset_t> localDeletions;
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
class HashIndex : public BaseHashIndex {

public:
    HashIndex(const StorageStructureIDAndFName& storageStructureIDAndFName,
        const DataType& keyDataType, BufferManager& bufferManager, WAL* wal);

public:
    // TODO(Guodong): consider only expose vector level API.
    bool lookup(
        Transaction* trx, ValueVector* keyVector, uint64_t vectorPos, node_offset_t& result);
    void deleteKey(Transaction* trx, ValueVector* keyVector, uint64_t vectorPos);
    bool insert(Transaction* trx, ValueVector* keyVector, uint64_t vectorPos, node_offset_t value);

    inline bool lookup(Transaction* transaction, int64_t key, node_offset_t& result) {
        return lookupInternal(transaction, reinterpret_cast<const uint8_t*>(&key), result);
    }
    // TODO(Guodong): Add the support of string keys back to fix this.
    inline bool lookup(Transaction* transaction, const char* key, node_offset_t& result) {
        return lookupInternal(transaction, reinterpret_cast<const uint8_t*>(key), result);
    }
    inline void deleteKey(Transaction* transaction, int64_t key) {
        deleteInternal(transaction, reinterpret_cast<const uint8_t*>(&key));
    }
    // TODO(Guodong): should we give trx for write?
    inline bool insert(Transaction* transaction, int64_t key, node_offset_t value) {
        return insertInternal(transaction, reinterpret_cast<const uint8_t*>(&key), value);
    }
    void prepareCommitOrRollbackIfNecessary(bool isCommit);
    void checkpointInMemoryIfNecessary();
    void rollbackInMemoryIfNecessary() const;
    inline VersionedFileHandle* getFileHandle() const { return fileHandle.get(); }

private:
    bool lookupInternal(Transaction* transaction, const uint8_t* key, node_offset_t& result);
    void deleteInternal(Transaction* transaction, const uint8_t* key) const;
    bool insertInternal(Transaction* transaction, const uint8_t* key, node_offset_t value);
    template<ChainedSlotsAction action>
    bool performActionInChainedSlots(TransactionType trxType, HashIndexHeader& header,
        SlotInfo& slotInfo, const uint8_t* key, node_offset_t& result);
    bool lookupInPersistentIndex(
        TransactionType trxType, const uint8_t* key, node_offset_t& result);
    // The following two functions are only used in prepareCommit, and are not thread-safe.
    void insertIntoPersistentIndex(const uint8_t* key, node_offset_t value);
    void deleteFromPersistentIndex(const uint8_t* key);

    void copyAndUpdateSlotHeader(bool isCopyEntry, Slot& slot, entry_pos_t entryPos,
        const uint8_t* key, node_offset_t value);
    void copyKVOrEntryToSlot(bool isCopyEntry, const SlotInfo& slotInfo, Slot& slot,
        const uint8_t* key, node_offset_t value);
    void splitSlot(HashIndexHeader& header);
    void rehashSlots(HashIndexHeader& header);
    vector<pair<SlotInfo, Slot>> getChainedSlots(slot_id_t pSlotId);
    void copyEntryToSlot(slot_id_t slotId, uint8_t* entry);

    void prepareCommit();
    void prepareRollback();

    entry_pos_t findMatchedEntryInSlot(const Slot& slot, const uint8_t* key) const;

    void loopChainedSlotsToFindOneWithFreeSpace(SlotInfo& slotInfo, Slot& slot);

    inline void updateSlot(const SlotInfo& slotInfo, Slot& slot) const {
        slotInfo.slotType == SlotType::PRIMARY ? pSlots->update(slotInfo.slotId, slot) :
                                                 oSlots->update(slotInfo.slotId, slot);
    }
    inline Slot getSlot(TransactionType trxType, const SlotInfo& slotInfo) const {
        return slotInfo.slotType == SlotType::PRIMARY ? pSlots->get(slotInfo.slotId, trxType) :
                                                        oSlots->get(slotInfo.slotId, trxType);
    }

public:
    StorageStructureIDAndFName storageStructureIDAndFName;
    BufferManager& bm;
    WAL* wal;
    unique_ptr<VersionedFileHandle> fileHandle;
    unique_ptr<BaseDiskArray<HashIndexHeader>> headerArray;
    unique_ptr<BaseDiskArray<Slot>> pSlots;
    unique_ptr<BaseDiskArray<Slot>> oSlots;
    insert_function_t keyInsertFunc;
    equals_function_t keyEqualsFunc;
    unique_ptr<DiskOverflowFile> diskOverflowFile;
    unique_ptr<HashIndexLocalStorage> localStorage;
};

} // namespace storage
} // namespace graphflow
