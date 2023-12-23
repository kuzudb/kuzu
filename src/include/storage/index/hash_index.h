#pragma once

#include <utility>

#include "hash_index_builder.h"
#include "storage/storage_structure/disk_overflow_file.h"

namespace kuzu {
namespace storage {

enum class HashIndexLocalLookupState : uint8_t { KEY_FOUND, KEY_DELETED, KEY_NOT_EXIST };
enum class ChainedSlotsAction : uint8_t { LOOKUP_IN_SLOTS, DELETE_IN_SLOTS, FIND_FREE_SLOT };

template<typename T>
class TemplatedHashIndexLocalStorage {
public:
    HashIndexLocalLookupState lookup(const T& key, common::offset_t& result);
    void deleteKey(const T& key);
    bool insert(const T& key, common::offset_t value);

    inline bool hasUpdates() const { return !(localInsertions.empty() && localDeletions.empty()); }
    inline void clear() {
        localInsertions.clear();
        localDeletions.clear();
    }

    std::unordered_map<T, common::offset_t> localInsertions;
    std::unordered_set<T> localDeletions;
};

// Local storage consists of two in memory indexes. One (localInsertionIndex) is to keep track of
// all newly inserted entries, and the other (localDeletionIndex) is to keep track of newly deleted
// entries (not available in localInsertionIndex). We assume that in a transaction, the insertions
// and deletions are very small, thus they can be kept in memory.
class HashIndexLocalStorage {
public:
    explicit HashIndexLocalStorage(common::LogicalType keyDataType)
        : keyDataType{std::move(keyDataType)} {}
    // Currently, we assume that reads(lookup) and writes(delete/insert) of the local storage will
    // never happen concurrently. Thus, lookup requires no local storage lock. Writes are
    // coordinated to execute in serial with the help of the localStorageMutex. This is a
    // simplification to the lock scheme, but can be relaxed later if necessary.
    HashIndexLocalLookupState lookup(const uint8_t* key, common::offset_t& result);
    void deleteKey(const uint8_t* key);
    bool insert(const uint8_t* key, common::offset_t value);
    void applyLocalChanges(const std::function<void(const uint8_t*)>& deleteOp,
        const std::function<void(const uint8_t*, common::offset_t)>& insertOp);

    bool hasUpdates() const;
    void clear();

public:
    std::shared_mutex localStorageSharedMutex;

private:
    common::LogicalType keyDataType;
    TemplatedHashIndexLocalStorage<int64_t> templatedLocalStorageForInt;
    TemplatedHashIndexLocalStorage<std::string> templatedLocalStorageForString;
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
    HashIndex(const DBFileIDAndName& dbFileIDAndName,
        const std::shared_ptr<BMFileHandle>& fileHandle,
        const std::shared_ptr<DiskOverflowFile>& overflowFile, uint64_t indexPos,
        const common::LogicalType& keyDataType, BufferManager& bufferManager, WAL* wal);

public:
    bool lookupInternal(
        transaction::Transaction* transaction, const uint8_t* key, common::offset_t& result);
    void deleteInternal(const uint8_t* key) const;
    bool insertInternal(const uint8_t* key, common::offset_t value);

    void prepareCommit();
    void prepareRollback();
    void checkpointInMemory();
    void rollbackInMemory();
    inline BMFileHandle* getFileHandle() const { return fileHandle.get(); }

private:
    template<ChainedSlotsAction action>
    bool performActionInChainedSlots(transaction::TransactionType trxType, HashIndexHeader& header,
        SlotInfo& slotInfo, const uint8_t* key, common::offset_t& result);
    bool lookupInPersistentIndex(
        transaction::TransactionType trxType, const uint8_t* key, common::offset_t& result);
    // The following two functions are only used in prepareCommit, and are not thread-safe.
    void insertIntoPersistentIndex(const uint8_t* key, common::offset_t value);
    void deleteFromPersistentIndex(const uint8_t* key);

    void copyAndUpdateSlotHeader(bool isCopyEntry, Slot<T>& slot, entry_pos_t entryPos,
        const uint8_t* key, common::offset_t value);
    void copyKVOrEntryToSlot(bool isCopyEntry, const SlotInfo& slotInfo, Slot<T>& slot,
        const uint8_t* key, common::offset_t value);
    void splitSlot(HashIndexHeader& header);
    void rehashSlots(HashIndexHeader& header);
    std::vector<std::pair<SlotInfo, Slot<T>>> getChainedSlots(slot_id_t pSlotId);
    void copyEntryToSlot(slot_id_t slotId, uint8_t* entry);

    entry_pos_t findMatchedEntryInSlot(
        transaction::TransactionType trxType, const Slot<T>& slot, const uint8_t* key) const;

    void loopChainedSlotsToFindOneWithFreeSpace(SlotInfo& slotInfo, Slot<T>& slot);

    inline void updateSlot(const SlotInfo& slotInfo, Slot<T>& slot) const {
        slotInfo.slotType == SlotType::PRIMARY ? pSlots->update(slotInfo.slotId, slot) :
                                                 oSlots->update(slotInfo.slotId, slot);
    }
    inline Slot<T> getSlot(transaction::TransactionType trxType, const SlotInfo& slotInfo) const {
        return slotInfo.slotType == SlotType::PRIMARY ? pSlots->get(slotInfo.slotId, trxType) :
                                                        oSlots->get(slotInfo.slotId, trxType);
    }

public:
    DBFileIDAndName dbFileIDAndName;
    BufferManager& bm;
    WAL* wal;
    std::shared_ptr<BMFileHandle> fileHandle;
    std::unique_ptr<BaseDiskArray<HashIndexHeader>> headerArray;
    std::unique_ptr<BaseDiskArray<Slot<T>>> pSlots;
    std::unique_ptr<BaseDiskArray<Slot<T>>> oSlots;
    insert_function_t keyInsertFunc;
    equals_function_t keyEqualsFunc;
    std::shared_ptr<DiskOverflowFile> diskOverflowFile;
    std::unique_ptr<HashIndexLocalStorage> localStorage;
    uint8_t slotCapacity;
};

class PrimaryKeyIndex {
public:
    PrimaryKeyIndex(const DBFileIDAndName& dbFileIDAndName, bool readOnly,
        const common::LogicalType& keyDataType, BufferManager& bufferManager, WAL* wal,
        common::VirtualFileSystem* vfs);

    bool lookup(transaction::Transaction* trx, const char* key, common::offset_t& result) {
        KU_ASSERT(keyDataTypeID == common::LogicalTypeID::STRING);
        return hashIndexForString[getHashIndexPosition(key)]->lookupInternal(
            trx, reinterpret_cast<const uint8_t*>(key), result);
    }
    bool lookup(transaction::Transaction* trx, int64_t key, common::offset_t& result) {
        KU_ASSERT(keyDataTypeID == common::LogicalTypeID::INT64);
        return hashIndexForInt64[getHashIndexPosition(key)]->lookupInternal(
            trx, reinterpret_cast<const uint8_t*>(&key), result);
    }
    bool lookup(transaction::Transaction* trx, common::ValueVector* keyVector, uint64_t vectorPos,
        common::offset_t& result);

    bool insert(const char* key, common::offset_t value) {
        KU_ASSERT(keyDataTypeID == common::LogicalTypeID::STRING);
        return hashIndexForString[getHashIndexPosition(key)]->insertInternal(
            reinterpret_cast<const uint8_t*>(key), value);
    }
    bool insert(int64_t key, common::offset_t value) {
        KU_ASSERT(keyDataTypeID == common::LogicalTypeID::INT64);
        return hashIndexForInt64[getHashIndexPosition(key)]->insertInternal(
            reinterpret_cast<const uint8_t*>(&key), value);
    }
    bool insert(common::ValueVector* keyVector, uint64_t vectorPos, common::offset_t value);

    void delete_(const char* key) {
        KU_ASSERT(keyDataTypeID == common::LogicalTypeID::STRING);
        return hashIndexForString[getHashIndexPosition(key)]->deleteInternal(
            reinterpret_cast<const uint8_t*>(key));
    }
    void delete_(int64_t key) {
        KU_ASSERT(keyDataTypeID == common::LogicalTypeID::INT64);
        return hashIndexForInt64[getHashIndexPosition(key)]->deleteInternal(
            reinterpret_cast<const uint8_t*>(&key));
    }
    void delete_(common::ValueVector* keyVector);

    void checkpointInMemory();
    void rollbackInMemory();
    void prepareCommit();
    void prepareRollback();
    BMFileHandle* getFileHandle() { return fileHandle.get(); }
    DiskOverflowFile* getDiskOverflowFile() { return overflowFile.get(); }

private:
    common::LogicalTypeID keyDataTypeID;
    std::shared_ptr<BMFileHandle> fileHandle;
    std::shared_ptr<DiskOverflowFile> overflowFile;
    std::vector<std::unique_ptr<HashIndex<int64_t>>> hashIndexForInt64;
    std::vector<std::unique_ptr<HashIndex<common::ku_string_t>>> hashIndexForString;
};

} // namespace storage
} // namespace kuzu
