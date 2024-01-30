#pragma once

#include "common/type_utils.h"
#include "common/types/ku_string.h"
#include "common/types/types.h"
#include "hash_index_header.h"
#include "hash_index_slot.h"
#include "storage/index/hash_index_utils.h"
#include "storage/storage_structure/disk_array.h"
#include "storage/storage_structure/disk_overflow_file.h"
#include "transaction/transaction.h"

namespace kuzu {
namespace storage {

template<typename T, typename S = T>
class HashIndexLocalStorage;

class OnDiskHashIndex {
public:
    virtual ~OnDiskHashIndex() = default;
    virtual void prepareCommit() = 0;
    virtual void prepareRollback() = 0;
    virtual void checkpointInMemory() = 0;
    virtual void rollbackInMemory() = 0;
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
//
// T is the key type used to access values
// S is the stored type, which is usually the same as T, with the exception of strings
template<typename T, typename S = T>
class HashIndex final : public OnDiskHashIndex {

public:
    HashIndex(const DBFileIDAndName& dbFileIDAndName,
        const std::shared_ptr<BMFileHandle>& fileHandle,
        const std::shared_ptr<DiskOverflowFile>& overflowFile, uint64_t indexPos,
        BufferManager& bufferManager, WAL* wal);

    ~HashIndex() override;

public:
    bool lookupInternal(transaction::Transaction* transaction, T key, common::offset_t& result);
    void deleteInternal(T key) const;
    bool insertInternal(T key, common::offset_t value);

    void prepareCommit() override;
    void prepareRollback() override;
    void checkpointInMemory() override;
    void rollbackInMemory() override;
    inline BMFileHandle* getFileHandle() const { return fileHandle.get(); }

private:
    bool lookupInPersistentIndex(
        transaction::TransactionType trxType, T key, common::offset_t& result);
    // The following two functions are only used in prepareCommit, and are not thread-safe.
    void insertIntoPersistentIndex(T key, common::offset_t value);
    void deleteFromPersistentIndex(T key);

    entry_pos_t findMatchedEntryInSlot(
        transaction::TransactionType trxType, const Slot<S>& slot, T key) const;

    inline void updateSlot(const SlotInfo& slotInfo, const Slot<S>& slot) {
        slotInfo.slotType == SlotType::PRIMARY ? pSlots->update(slotInfo.slotId, slot) :
                                                 oSlots->update(slotInfo.slotId, slot);
    }

    inline Slot<S> getSlot(transaction::TransactionType trxType, const SlotInfo& slotInfo) {
        return slotInfo.slotType == SlotType::PRIMARY ? pSlots->get(slotInfo.slotId, trxType) :
                                                        oSlots->get(slotInfo.slotId, trxType);
    }

    inline uint32_t appendPSlot() { return pSlots->pushBack(Slot<S>{}); }

    inline uint64_t appendOverflowSlot(Slot<S>&& newSlot) { return oSlots->pushBack(newSlot); }

    inline void splitSlot(HashIndexHeader& header) {
        appendPSlot();
        rehashSlots(header);
        header.incrementNextSplitSlotId();
    }
    void rehashSlots(HashIndexHeader& header);

    inline bool equals(
        transaction::TransactionType /*trxType*/, T keyToLookup, const S& keyInEntry) const {
        return keyToLookup == keyInEntry;
    }
    template<typename K, bool isCopyEntry>
    void copyAndUpdateSlotHeader(
        Slot<S>& slot, entry_pos_t entryPos, K key, common::offset_t value) {
        if constexpr (isCopyEntry) {
            memcpy(slot.entries[entryPos].data, &key, this->indexHeader->numBytesPerEntry);
        } else {
            insert(key, slot.entries[entryPos].data, value);
        }
        slot.header.setEntryValid(entryPos);
        slot.header.numEntries++;
    }

    inline void insert(T key, uint8_t* entry, common::offset_t offset) {
        memcpy(entry, &key, sizeof(T));
        memcpy(entry + sizeof(T), &offset, sizeof(common::offset_t));
    }
    inline common::hash_t hashStored(transaction::TransactionType /*trxType*/, const S& key) const {
        return HashIndexUtils::hash(key);
    }

    struct SlotIterator {
        SlotInfo slotInfo;
        Slot<S> slot;
    };

    SlotIterator getSlotIterator(slot_id_t slotId, transaction::TransactionType trxType) {
        return SlotIterator{SlotInfo{slotId, SlotType::PRIMARY},
            getSlot(trxType, SlotInfo{slotId, SlotType::PRIMARY})};
    }

    bool nextChainedSlot(transaction::TransactionType trxType, SlotIterator& iter) {
        if (iter.slot.header.nextOvfSlotId != 0) {
            iter.slotInfo.slotId = iter.slot.header.nextOvfSlotId;
            iter.slotInfo.slotType = SlotType::OVF;
            iter.slot = getSlot(trxType, iter.slotInfo);
            return true;
        }
        return false;
    }

    std::vector<std::pair<SlotInfo, Slot<S>>> getChainedSlots(slot_id_t pSlotId);

    template<typename K, bool isCopyEntry>
    void copyKVOrEntryToSlot(
        const SlotInfo& slotInfo, Slot<S>& slot, K key, common::offset_t value) {
        if (slot.header.numEntries == getSlotCapacity<S>()) {
            // Allocate a new oSlot, insert the entry to the new oSlot, and update slot's
            // nextOvfSlotId.
            Slot<S> newSlot;
            auto entryPos = 0u; // Always insert to the first entry when there is a new slot.
            copyAndUpdateSlotHeader<K, isCopyEntry>(newSlot, entryPos, key, value);
            slot.header.nextOvfSlotId = appendOverflowSlot(std::move(newSlot));
        } else {
            for (auto entryPos = 0u; entryPos < getSlotCapacity<S>(); entryPos++) {
                if (!slot.header.isEntryValid(entryPos)) {
                    copyAndUpdateSlotHeader<K, isCopyEntry>(slot, entryPos, key, value);
                    break;
                }
            }
        }
        updateSlot(slotInfo, slot);
    }

    void copyEntryToSlot(slot_id_t slotId, const S& entry);

private:
    // Local Storage for strings stores an std::string, but still takes std::string_view as keys to
    // avoid unnecessary copying
    using LocalStorageType =
        typename std::conditional<std::is_same<T, std::string_view>::value, std::string, T>::type;
    DBFileIDAndName dbFileIDAndName;
    BufferManager& bm;
    WAL* wal;
    std::shared_ptr<BMFileHandle> fileHandle;
    std::unique_ptr<BaseDiskArray<HashIndexHeader>> headerArray;
    std::unique_ptr<BaseDiskArray<Slot<S>>> pSlots;
    std::unique_ptr<BaseDiskArray<Slot<S>>> oSlots;
    std::shared_ptr<DiskOverflowFile> diskOverflowFile;
    std::unique_ptr<HashIndexLocalStorage<T, LocalStorageType>> localStorage;
    std::unique_ptr<HashIndexHeader> indexHeader;
};

template<>
common::hash_t HashIndex<std::string_view, common::ku_string_t>::hashStored(
    transaction::TransactionType trxType, const common::ku_string_t& key) const;

template<>
inline bool HashIndex<std::string_view, common::ku_string_t>::equals(
    transaction::TransactionType trxType, std::string_view keyToLookup,
    const common::ku_string_t& keyInEntry) const;

class PrimaryKeyIndex {
public:
    PrimaryKeyIndex(const DBFileIDAndName& dbFileIDAndName, bool readOnly,
        common::PhysicalTypeID keyDataType, BufferManager& bufferManager, WAL* wal,
        common::VirtualFileSystem* vfs);

    ~PrimaryKeyIndex();

    template<typename T, typename S = T>
    inline HashIndex<T, S>* getTypedHashIndex(T key) {
        return common::ku_dynamic_cast<OnDiskHashIndex*, HashIndex<T, S>*>(
            hashIndices[getHashIndexPosition(key)].get());
    }

    inline bool lookup(
        transaction::Transaction* trx, common::ku_string_t key, common::offset_t& result) {
        return lookup(trx, key.getAsStringView(), result);
    }
    inline bool lookup(
        transaction::Transaction* trx, std::string_view key, common::offset_t& result) {
        KU_ASSERT(keyDataTypeID == common::PhysicalTypeID::STRING);
        return getTypedHashIndex<std::string_view, common::ku_string_t>(key)->lookupInternal(
            trx, key, result);
    }
    template<common::HashablePrimitive T>
    inline bool lookup(transaction::Transaction* trx, T key, common::offset_t& result) {
        KU_ASSERT(keyDataTypeID == common::TypeUtils::getPhysicalTypeIDForType<T>());
        return getTypedHashIndex(key)->lookupInternal(trx, key, result);
    }

    bool lookup(transaction::Transaction* trx, common::ValueVector* keyVector, uint64_t vectorPos,
        common::offset_t& result);

    inline bool insert(common::ku_string_t key, common::offset_t value) {
        return insert(key.getAsStringView(), value);
    }
    inline bool insert(std::string_view key, common::offset_t value) {
        KU_ASSERT(keyDataTypeID == common::PhysicalTypeID::STRING);
        return getTypedHashIndex<std::string_view, common::ku_string_t>(key)->insertInternal(
            key, value);
    }
    template<common::HashablePrimitive T>
    inline bool insert(T key, common::offset_t value) {
        KU_ASSERT(keyDataTypeID == common::TypeUtils::getPhysicalTypeIDForType<T>());
        return getTypedHashIndex(key)->insertInternal(key, value);
    }
    bool insert(common::ValueVector* keyVector, uint64_t vectorPos, common::offset_t value);

    inline void delete_(common::ku_string_t key) { return delete_(key.getAsStringView()); }
    inline void delete_(std::string_view key) {
        KU_ASSERT(keyDataTypeID == common::PhysicalTypeID::STRING);
        return getTypedHashIndex<std::string_view, common::ku_string_t>(key)->deleteInternal(key);
    }
    template<common::HashablePrimitive T>
    inline void delete_(T key) {
        KU_ASSERT(keyDataTypeID == common::TypeUtils::getPhysicalTypeIDForType<T>());
        return getTypedHashIndex(key)->deleteInternal(key);
    }

    void delete_(common::ValueVector* keyVector);

    void checkpointInMemory();
    void rollbackInMemory();
    void prepareCommit();
    void prepareRollback();
    BMFileHandle* getFileHandle() { return fileHandle.get(); }
    DiskOverflowFile* getDiskOverflowFile() { return overflowFile.get(); }

private:
    common::PhysicalTypeID keyDataTypeID;
    std::shared_ptr<BMFileHandle> fileHandle;
    std::shared_ptr<DiskOverflowFile> overflowFile;
    std::vector<std::unique_ptr<OnDiskHashIndex>> hashIndices;
};

} // namespace storage
} // namespace kuzu
