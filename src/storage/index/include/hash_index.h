#pragma once

#include "hash_index_builder.h"

#include "src/function/hash/operations/include/hash_operations.h"
#include "src/storage/storage_structure/include/disk_overflow_file.h"

using namespace graphflow::common;

namespace graphflow {
namespace storage {

enum class HashIndexLocalLookupState { KEY_FOUND, KEY_DELETED, KEY_NOT_EXIST };

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

private:
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
    inline bool insert(Transaction* transaction, int64_t key, node_offset_t value) {
        return insertInternal(transaction, reinterpret_cast<const uint8_t*>(&key), value);
    }

private:
    bool lookupInternal(Transaction* transaction, const uint8_t* key, node_offset_t& result);
    void deleteInternal(Transaction* transaction, const uint8_t* key);
    bool insertInternal(Transaction* transaction, const uint8_t* key, node_offset_t value);
    bool lookupInPersistentIndex(const uint8_t* key, node_offset_t& result);

    bool lookupInSlot(Slot* slot, const uint8_t* key, node_offset_t& result) const;
    inline Slot* getSlot(const SlotInfo& slotInfo) const {
        return slotInfo.isPSlot ? &pSlots->operator[](slotInfo.slotId) :
                                  &oSlots->operator[](slotInfo.slotId);
    }

private:
    BufferManager& bm;
    unique_ptr<VersionedFileHandle> fileHandle;
    unique_ptr<InMemDiskArray<HashIndexHeader>> headerArray;
    unique_ptr<InMemDiskArray<Slot>> pSlots;
    unique_ptr<InMemDiskArray<Slot>> oSlots;
    equals_function_t keyEqualsFunc;
    unique_ptr<DiskOverflowFile> diskOverflowFile;
    unique_ptr<HashIndexLocalStorage> localStorage;
};

} // namespace storage
} // namespace graphflow
