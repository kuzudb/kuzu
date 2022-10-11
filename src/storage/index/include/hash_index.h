#pragma once

#include <bitset>
#include <climits>
#include <iostream>

#include "hash_index_utils.h"
#include "in_mem_hash_index.h"

#include "src/common/include/configs.h"
#include "src/common/include/file_utils.h"
#include "src/common/include/vector/value_vector.h"
#include "src/function/hash/operations/include/hash_operations.h"
#include "src/storage/buffer_manager/include/buffer_manager.h"
#include "src/storage/buffer_manager/include/memory_manager.h"
#include "src/storage/storage_structure/include/disk_overflow_file.h"

using namespace graphflow::common;

namespace graphflow {
namespace storage {

enum class HashIndexLocalLookupState { KEY_FOUND, KEY_DELETED, KEY_NOT_EXIST };

// Local storage consists of two in memory indexes. One (localInsertionIndex) is to keep track of
// all newly inserted entries, and the other (localDeletionIndex) is to keep track of newly deleted
// entries (not available in localInsertionIndex). We assume that in a transaction, the insertions
// and deletions are very small, thus they can be kept in memory.
class HashIndexLocalStorage {
    static const node_offset_t NODE_OFFSET_PLACE_HOLDER = UINT64_MAX;

public:
    explicit HashIndexLocalStorage(const DataType& keyDataType) {
        localInsertionIndex = make_unique<InMemHashIndex>(IN_MEM_TEMP_FILE_PATH, keyDataType);
        localDeletionIndex = make_unique<InMemHashIndex>(IN_MEM_TEMP_FILE_PATH, keyDataType);
    }

    // Currently, we assume that reads(lookup) and writes(delete/insert) of the local storage will
    // never happen concurrently. Thus, lookup requires no local storage lock. Writes are
    // coordinated to execute in serial with the help of the localStorageMutex. This is a
    // simplification to the lock scheme, but can be relaxed later if necessary.
    HashIndexLocalLookupState lookup(const uint8_t* key, node_offset_t& result);
    void deleteKey(const uint8_t* key);
    bool insert(const uint8_t* key, node_offset_t value);

private:
    shared_mutex localStorageSharedMutex;
    unique_ptr<InMemHashIndex> localInsertionIndex;
    unique_ptr<InMemHashIndex> localDeletionIndex;
};

// HashIndex is the entrance to handle all updates and lookups into the index after building from
// scratch through InMemHashIndex.
// The index consists of two parts, one is the persistent storage (from the persistent index file),
// and the other is the local storage. All lookups/deletions/insertions go through local storage,
// and then the persistent storage if necessary.
class HashIndex : public BaseHashIndex {

public:
    HashIndex(const StorageStructureIDAndFName& storageStructureIDAndFName,
        const DataType& keyDataType, BufferManager& bufferManager, bool isInMemory);

    ~HashIndex();

public:
    inline bool lookup(Transaction* transaction, int64_t key, node_offset_t& result) {
        return lookupInternal(transaction, reinterpret_cast<const uint8_t*>(&key), result);
    }
    inline bool lookup(Transaction* transaction, const char* key, node_offset_t& result) {
        return lookupInternal(transaction, reinterpret_cast<const uint8_t*>(key), result);
    }
    inline void deleteKey(Transaction* transaction, int64_t key) {
        deleteInternal(transaction, reinterpret_cast<const uint8_t*>(&key));
    }
    inline void deleteKey(Transaction* transaction, const char* key) {
        deleteInternal(transaction, reinterpret_cast<const uint8_t*>(key));
    }
    inline bool insert(Transaction* transaction, int64_t key, node_offset_t value) {
        return insertInternal(transaction, reinterpret_cast<const uint8_t*>(&key), value);
    }
    inline bool insert(Transaction* transaction, const char* key, node_offset_t value) {
        return insertInternal(transaction, reinterpret_cast<const uint8_t*>(key), value);
    }

private:
    void initializeFileAndHeader(const DataType& keyDataType);
    void readPageMapper(vector<page_idx_t>& mapper, page_idx_t mapperFirstPageIdx);

    bool lookupInternal(Transaction* transaction, const uint8_t* key, node_offset_t& result);
    void deleteInternal(Transaction* transaction, const uint8_t* key);
    bool insertInternal(Transaction* transaction, const uint8_t* key, node_offset_t value);
    bool lookupInPersistentIndex(const uint8_t* key, node_offset_t& result);

    bool lookupInSlot(uint8_t* slot, const uint8_t* key, node_offset_t& result) const;
    uint8_t* pinSlot(const SlotInfo& slotInfo);
    void unpinSlot(const SlotInfo& slotInfo);

private:
    StorageStructureIDAndFName storageStructureIDAndFName;
    bool isInMemory;
    unique_ptr<FileHandle> fh;
    BufferManager& bm;
    equals_function_t keyEqualsFunc;
    unique_ptr<DiskOverflowFile> diskOverflowFile;
    unique_ptr<HashIndexLocalStorage> localStorage;
};

} // namespace storage
} // namespace graphflow
