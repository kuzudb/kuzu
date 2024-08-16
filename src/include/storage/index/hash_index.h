#pragma once

#include <algorithm>
#include <cstdint>
#include <string_view>
#include <type_traits>

#include "common/cast.h"
#include "common/type_utils.h"
#include "common/types/ku_string.h"
#include "common/types/types.h"
#include "hash_index_header.h"
#include "hash_index_slot.h"
#include "storage/index/hash_index_utils.h"
#include "storage/index/in_mem_hash_index.h"
#include "storage/local_storage/local_hash_index.h"

namespace kuzu {
namespace common {
class VirtualFileSystem;
}
namespace transaction {
class Transaction;
enum class TransactionType : uint8_t;
} // namespace transaction
namespace storage {

class BMFileHandle;
class BufferManager;
class OverflowFileHandle;
template<typename T>
class DiskArray;
class DiskArrayCollection;

class OnDiskHashIndex {
public:
    virtual ~OnDiskHashIndex() = default;
    virtual bool checkpoint() = 0;
    virtual void prepareRollback() = 0;
    virtual bool checkpointInMemory() = 0;
    virtual bool rollbackInMemory() = 0;
    virtual void bulkReserve(uint64_t numValuesToAppend) = 0;
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
template<typename T>
class HashIndex final : public OnDiskHashIndex {
public:
    HashIndex(const DBFileIDAndName& dbFileIDAndName, BMFileHandle* fileHandle,
        OverflowFileHandle* overflowFileHandle, DiskArrayCollection& diskArrays, uint64_t indexPos,
        ShadowFile* shadowFile, const HashIndexHeader& indexHeaderForReadTrx,
        HashIndexHeader& indexHeaderForWriteTrx);

    ~HashIndex() override;

public:
    using Key =
        typename std::conditional<std::same_as<T, common::ku_string_t>, std::string_view, T>::type;
    // For read transactions, local storage is skipped, lookups are performed on the persistent
    // storage. For write transactions, lookups are performed in the local storage first, then in
    // the persistent storage if necessary. In details, there are three cases for the local storage
    // lookup:
    // - the key is found in the local storage, directly return true;
    // - the key has been marked as deleted in the local storage, return false;
    // - the key is neither deleted nor found in the local storage, lookup in the persistent
    // storage.
    bool lookupInternal(const transaction::Transaction* transaction, Key key,
        common::offset_t& result, visible_func isVisible) {
        KU_ASSERT(transaction->getType() != transaction::TransactionType::CHECKPOINT);
        auto localLookupState = localStorage->lookup(key, result, isVisible);
        if (localLookupState == HashIndexLocalLookupState::KEY_DELETED) {
            return false;
        }
        if (localLookupState == HashIndexLocalLookupState::KEY_FOUND) {
            return true;
        }
        KU_ASSERT(localLookupState == HashIndexLocalLookupState::KEY_NOT_EXIST);
        return lookupInPersistentIndex(transaction, key, result, isVisible);
    }

    // For deletions, we don't check if the deleted keys exist or not. Thus, we don't need to check
    // in the persistent storage and directly delete keys in the local storage.
    void deleteInternal(Key key) const { localStorage->deleteKey(key); }

    // For insertions, we first check in the local storage. There are three cases:
    // - the key is found in the local storage, return false;
    // - the key is marked as deleted in the local storage, insert the key to the local storage;
    // - the key doesn't exist in the local storage, check if the key exists in the persistent
    // index, if
    //   so, return false, else insert the key to the local storage.
    bool insertInternal(const transaction::Transaction* transaction, Key key,
        common::offset_t value, visible_func isVisible) {
        common::offset_t tmpResult;
        auto localLookupState = localStorage->lookup(key, tmpResult, isVisible);
        if (localLookupState == HashIndexLocalLookupState::KEY_FOUND) {
            return false;
        }
        if (localLookupState != HashIndexLocalLookupState::KEY_DELETED) {
            if (lookupInPersistentIndex(transaction, key, tmpResult, isVisible)) {
                return false;
            }
        }
        return localStorage->insert(key, value, isVisible);
    }

    using BufferKeyType =
        typename std::conditional<std::same_as<T, common::ku_string_t>, std::string, T>::type;
    // Appends the buffer to the index. Returns the number of values successfully inserted,
    // or the index of the first value which cannot be inserted.
    size_t append(const transaction::Transaction* transaction,
        const IndexBuffer<BufferKeyType>& buffer, visible_func isVisible) {
        // Check if values already exist in persistent storage
        if (indexHeaderForWriteTrx.numEntries > 0) {
            common::offset_t result;
            for (size_t i = 0; i < buffer.size(); i++) {
                const auto& [key, value] = buffer[i];
                if (lookupInPersistentIndex(transaction, key, result, isVisible)) {
                    return i;
                }
            }
        }
        return localStorage->append(buffer, isVisible);
    }

    bool checkpoint() override;
    void prepareRollback() override;
    bool checkpointInMemory() override;
    bool rollbackInMemory() override;
    inline BMFileHandle* getFileHandle() const { return fileHandle; }

private:
    bool lookupInPersistentIndex(const transaction::Transaction* transaction, Key key,
        common::offset_t& result, visible_func isVisible) {
        auto& header = transaction->getType() == transaction::TransactionType::CHECKPOINT ?
                           this->indexHeaderForWriteTrx :
                           this->indexHeaderForReadTrx;
        // There may not be any primary key slots if we try to lookup on an empty index
        if (header.numEntries == 0) {
            return false;
        }
        auto hashValue = HashIndexUtils::hash(key);
        auto fingerprint = HashIndexUtils::getFingerprintForHash(hashValue);
        auto iter = getSlotIterator(HashIndexUtils::getPrimarySlotIdForHash(header, hashValue),
            transaction);
        do {
            auto entryPos =
                findMatchedEntryInSlot(transaction, iter.slot, key, fingerprint, isVisible);
            if (entryPos != SlotHeader::INVALID_ENTRY_POS) {
                result = iter.slot.entries[entryPos].value;
                return true;
            }
        } while (nextChainedSlot(transaction, iter));
        return false;
    }
    void deleteFromPersistentIndex(const transaction::Transaction* transaction, Key key,
        visible_func isVisible);

    entry_pos_t findMatchedEntryInSlot(const transaction::Transaction* transaction,
        const Slot<T>& slot, Key key, uint8_t fingerprint, const visible_func& isVisible) const {
        for (auto entryPos = 0u; entryPos < getSlotCapacity<T>(); entryPos++) {
            if (slot.header.isEntryValid(entryPos) &&
                slot.header.fingerprints[entryPos] == fingerprint &&
                equals(transaction, key, slot.entries[entryPos].key) &&
                isVisible(slot.entries[entryPos].value)) {
                return entryPos;
            }
        }
        return SlotHeader::INVALID_ENTRY_POS;
    }

    inline void updateSlot(const transaction::Transaction* transaction, const SlotInfo& slotInfo,
        const Slot<T>& slot) {
        slotInfo.slotType == SlotType::PRIMARY ?
            pSlots->update(transaction, slotInfo.slotId, slot) :
            oSlots->update(transaction, slotInfo.slotId, slot);
    }

    inline Slot<T> getSlot(const transaction::Transaction* transaction,
        const SlotInfo& slotInfo) const {
        return slotInfo.slotType == SlotType::PRIMARY ? pSlots->get(slotInfo.slotId, transaction) :
                                                        oSlots->get(slotInfo.slotId, transaction);
    }

    inline uint32_t appendPSlot(const transaction::Transaction* transaction) {
        return pSlots->pushBack(transaction, Slot<T>{});
    }

    inline uint64_t appendOverflowSlot(const transaction::Transaction* transaction,
        Slot<T>&& newSlot) {
        return oSlots->pushBack(transaction, newSlot);
    }

    void splitSlots(const transaction::Transaction* transaction, HashIndexHeader& header,
        slot_id_t numSlotsToSplit);

    // Resizes the local storage to support the given number of new entries
    void bulkReserve(uint64_t newEntries) override;
    // Resizes the on-disk index to support the given number of new entries
    void reserve(const transaction::Transaction* transaction, uint64_t newEntries);

    struct HashIndexEntryView {
        slot_id_t diskSlotId;
        uint8_t fingerprint;
        const SlotEntry<T>* entry;
    };

    void sortEntries(const transaction::Transaction* transaction,
        const InMemHashIndex<T>& insertLocalStorage,
        typename InMemHashIndex<T>::SlotIterator& slotToMerge,
        std::vector<HashIndexEntryView>& partitions);
    void mergeBulkInserts(const transaction::Transaction* transaction,
        const InMemHashIndex<T>& insertLocalStorage);
    // Returns the number of elements merged which matched the given slot id
    size_t mergeSlot(const transaction::Transaction* transaction,
        const std::vector<HashIndexEntryView>& slotToMerge,
        typename DiskArray<Slot<T>>::WriteIterator& diskSlotIterator,
        typename DiskArray<Slot<T>>::WriteIterator& diskOverflowSlotIterator, slot_id_t slotId);

    inline bool equals(const transaction::Transaction* /*transaction*/, Key keyToLookup,
        const T& keyInEntry) const {
        return keyToLookup == keyInEntry;
    }

    inline common::hash_t hashStored(const transaction::Transaction* /*transaction*/,
        const T& key) const {
        return HashIndexUtils::hash(key);
    }

    struct SlotIterator {
        SlotInfo slotInfo;
        Slot<T> slot;
    };

    SlotIterator getSlotIterator(slot_id_t slotId, const transaction::Transaction* transaction) {
        return SlotIterator{SlotInfo{slotId, SlotType::PRIMARY},
            getSlot(transaction, SlotInfo{slotId, SlotType::PRIMARY})};
    }

    bool nextChainedSlot(const transaction::Transaction* transaction, SlotIterator& iter) const {
        KU_ASSERT(iter.slotInfo.slotType == SlotType::PRIMARY ||
                  iter.slotInfo.slotId != iter.slot.header.nextOvfSlotId);
        if (iter.slot.header.nextOvfSlotId != SlotHeader::INVALID_OVERFLOW_SLOT_ID) {
            iter.slotInfo.slotId = iter.slot.header.nextOvfSlotId;
            iter.slotInfo.slotType = SlotType::OVF;
            iter.slot = getSlot(transaction, iter.slotInfo);
            return true;
        }
        return false;
    }

    std::vector<std::pair<SlotInfo, Slot<T>>> getChainedSlots(
        const transaction::Transaction* transaction, slot_id_t pSlotId);

private:
    DBFileIDAndName dbFileIDAndName;
    ShadowFile* shadowFile;
    uint64_t headerPageIdx;
    BMFileHandle* fileHandle;
    std::unique_ptr<DiskArray<Slot<T>>> pSlots;
    std::unique_ptr<DiskArray<Slot<T>>> oSlots;
    OverflowFileHandle* overflowFileHandle;
    std::unique_ptr<HashIndexLocalStorage<T>> localStorage;
    const HashIndexHeader& indexHeaderForReadTrx;
    HashIndexHeader& indexHeaderForWriteTrx;
};

template<>
common::hash_t HashIndex<common::ku_string_t>::hashStored(
    const transaction::Transaction* transaction, const common::ku_string_t& key) const;

template<>
inline bool HashIndex<common::ku_string_t>::equals(const transaction::Transaction* transaction,
    std::string_view keyToLookup, const common::ku_string_t& keyInEntry) const {
    if (HashIndexUtils::areStringPrefixAndLenEqual(keyToLookup, keyInEntry)) {
        auto entryKeyString = overflowFileHandle->readString(transaction->getType(), keyInEntry);
        return memcmp(keyToLookup.data(), entryKeyString.c_str(), entryKeyString.length()) == 0;
    }
    return false;
}

class PrimaryKeyIndex {
public:
    PrimaryKeyIndex(const DBFileIDAndName& dbFileIDAndName, bool readOnly, bool inMemMode,
        common::PhysicalTypeID keyDataType, BufferManager& bufferManager, ShadowFile* shadowFile,
        common::VirtualFileSystem* vfs, main::ClientContext* context);

    ~PrimaryKeyIndex();

    template<typename T>
    inline HashIndex<HashIndexType<T>>* getTypedHashIndex(T key) {
        return common::ku_dynamic_cast<OnDiskHashIndex*, HashIndex<HashIndexType<T>>*>(
            hashIndices[HashIndexUtils::getHashIndexPosition(key)].get());
    }
    template<common::IndexHashable T>
    inline HashIndex<T>* getTypedHashIndexByPos(uint64_t indexPos) {
        return common::ku_dynamic_cast<OnDiskHashIndex*, HashIndex<HashIndexType<T>>*>(
            hashIndices[indexPos].get());
    }

    inline bool lookup(const transaction::Transaction* trx, common::ku_string_t key,
        common::offset_t& result, visible_func isVisible) {
        return lookup(trx, key.getAsStringView(), result, isVisible);
    }
    template<common::IndexHashable T>
    inline bool lookup(const transaction::Transaction* trx, T key, common::offset_t& result,
        visible_func isVisible) {
        KU_ASSERT(keyDataTypeID == common::TypeUtils::getPhysicalTypeIDForType<T>());
        return getTypedHashIndex(key)->lookupInternal(trx, key, result, isVisible);
    }

    bool lookup(const transaction::Transaction* trx, common::ValueVector* keyVector,
        uint64_t vectorPos, common::offset_t& result, visible_func isVisible);

    inline bool insert(const transaction::Transaction* transaction, common::ku_string_t key,
        common::offset_t value, visible_func isVisible) {
        return insert(transaction, key.getAsStringView(), value, isVisible);
    }
    template<common::IndexHashable T>
    inline bool insert(const transaction::Transaction* transaction, T key, common::offset_t value,
        visible_func isVisible) {
        KU_ASSERT(keyDataTypeID == common::TypeUtils::getPhysicalTypeIDForType<T>());
        return getTypedHashIndex(key)->insertInternal(transaction, key, value, isVisible);
    }
    bool insert(const transaction::Transaction* transaction, common::ValueVector* keyVector,
        uint64_t vectorPos, common::offset_t value, visible_func isVisible);

    // Appends the buffer to the index. Returns the number of values successfully inserted.
    // If a key fails to insert, it immediately returns without inserting any more values,
    // and the returned value is also the index of the key which failed to insert.
    template<common::IndexHashable T>
    size_t appendWithIndexPos(const transaction::Transaction* transaction,
        const IndexBuffer<T>& buffer, uint64_t indexPos, visible_func isVisible) {
        KU_ASSERT(keyDataTypeID == common::TypeUtils::getPhysicalTypeIDForType<T>());
        KU_ASSERT(std::all_of(buffer.begin(), buffer.end(), [&](auto& elem) {
            return HashIndexUtils::getHashIndexPosition(elem.first) == indexPos;
        }));
        return getTypedHashIndexByPos<HashIndexType<T>>(indexPos)->append(transaction, buffer,
            isVisible);
    }

    void bulkReserve(uint64_t numValuesToAppend) {
        uint32_t eachSize = numValuesToAppend / NUM_HASH_INDEXES + 1;
        for (auto i = 0u; i < NUM_HASH_INDEXES; i++) {
            hashIndices[i]->bulkReserve(eachSize);
        }
    }

    inline void delete_(common::ku_string_t key) { return delete_(key.getAsStringView()); }
    template<common::IndexHashable T>
    inline void delete_(T key) {
        KU_ASSERT(keyDataTypeID == common::TypeUtils::getPhysicalTypeIDForType<T>());
        return getTypedHashIndex(key)->deleteInternal(key);
    }

    void delete_(common::ValueVector* keyVector);

    void checkpointInMemory();
    void checkpoint();
    BMFileHandle* getFileHandle() const { return fileHandle; }
    OverflowFile* getOverflowFile() const { return overflowFile.get(); }

    common::PhysicalTypeID keyTypeID() const { return keyDataTypeID; }

    void writeHeaders();

private:
    common::PhysicalTypeID keyDataTypeID;
    BMFileHandle* fileHandle;
    std::unique_ptr<OverflowFile> overflowFile;
    std::vector<std::unique_ptr<OnDiskHashIndex>> hashIndices;
    std::vector<HashIndexHeader> hashIndexHeadersForReadTrx;
    std::vector<HashIndexHeader> hashIndexHeadersForWriteTrx;
    DBFileIDAndName dbFileIDAndName;
    ShadowFile& shadowFile;
    // Stores both primary and overflow slots
    std::unique_ptr<DiskArrayCollection> hashIndexDiskArrays;
};

} // namespace storage
} // namespace kuzu
