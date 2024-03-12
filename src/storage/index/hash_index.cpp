#include "storage/index/hash_index.h"

#include <cstdint>
#include <type_traits>

#include "common/assert.h"
#include "common/string_utils.h"
#include "common/type_utils.h"
#include "common/types/int128_t.h"
#include "common/types/internal_id_t.h"
#include "common/types/ku_string.h"
#include "common/types/types.h"
#include "common/vector/value_vector.h"
#include "storage/buffer_manager/bm_file_handle.h"
#include "storage/file_handle.h"
#include "storage/index/hash_index_header.h"
#include "storage/index/hash_index_slot.h"
#include "storage/index/hash_index_utils.h"
#include "storage/index/in_mem_hash_index.h"
#include "storage/storage_structure/disk_array.h"
#include "transaction/transaction.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

enum class HashIndexLocalLookupState : uint8_t { KEY_FOUND, KEY_DELETED, KEY_NOT_EXIST };

// Local storage consists of two in memory indexes. One (localInsertionIndex) is to keep track of
// all newly inserted entries, and the other (localDeletionIndex) is to keep track of newly deleted
// entries (not available in localInsertionIndex). We assume that in a transaction, the insertions
// and deletions are very small, thus they can be kept in memory.
template<typename T>
class HashIndexLocalStorage {
public:
    using Key = typename std::conditional<std::same_as<T, std::string>, std::string_view, T>::type;
    HashIndexLocalLookupState lookup(Key key, common::offset_t& result) {
        if (localDeletions.contains(key)) {
            return HashIndexLocalLookupState::KEY_DELETED;
        }
        auto elem = localInsertions.find(key);
        if (elem != localInsertions.end()) {
            result = elem->second;
            return HashIndexLocalLookupState::KEY_FOUND;
        } else {
            return HashIndexLocalLookupState::KEY_NOT_EXIST;
        }
    }

    void deleteKey(Key key) {
        auto iter = localInsertions.find(key);
        if (iter != localInsertions.end()) {
            localInsertions.erase(iter);
        } else {
            localDeletions.insert(static_cast<T>(key));
        }
    }

    bool insert(Key key, common::offset_t value) {
        auto iter = localDeletions.find(key);
        if (iter != localDeletions.end()) {
            localDeletions.erase(iter);
        }
        if (localInsertions.contains(key)) {
            return false;
        }
        localInsertions[static_cast<T>(key)] = value;
        return true;
    }

    inline bool hasUpdates() const { return !(localInsertions.empty() && localDeletions.empty()); }

    inline int64_t getNetInserts() const {
        return static_cast<int64_t>(localInsertions.size()) - localDeletions.size();
    }

    inline void clear() {
        localInsertions.clear();
        localDeletions.clear();
    }

    void applyLocalChanges(const std::function<void(T)>& deleteOp,
        const std::function<void(T, common::offset_t)>& insertOp) {
        for (auto& key : localDeletions) {
            deleteOp(key);
        }
        for (auto& [key, val] : localInsertions) {
            insertOp(key, val);
        }
    }

private:
    // When the storage type is string, allow the key type to be string_view with a custom hash
    // function
    using hash_function = typename std::conditional<std::is_same<T, std::string>::value,
        common::StringUtils::string_hash, std::hash<T>>::type;
    std::unordered_map<T, common::offset_t, hash_function, std::equal_to<>> localInsertions;
    std::unordered_set<T, hash_function, std::equal_to<>> localDeletions;
};

template<typename T>
HashIndex<T>::HashIndex(const DBFileIDAndName& dbFileIDAndName,
    const std::shared_ptr<BMFileHandle>& fileHandle, OverflowFileHandle* overflowFileHandle,
    uint64_t indexPos, BufferManager& bufferManager, WAL* wal)
    : dbFileIDAndName{dbFileIDAndName}, bm{bufferManager}, wal{wal}, fileHandle(fileHandle),
      overflowFileHandle(overflowFileHandle), bulkInsertLocalStorage{overflowFileHandle} {
    // TODO: Handle data not existing
    headerArray = std::make_unique<BaseDiskArray<HashIndexHeader>>(*fileHandle,
        dbFileIDAndName.dbFileID, NUM_HEADER_PAGES * indexPos + INDEX_HEADER_ARRAY_HEADER_PAGE_IDX,
        &bm, wal, Transaction::getDummyReadOnlyTrx().get(), true /*bypassWAL*/);
    // Read indexHeader from the headerArray, which contains only one element.
    this->indexHeaderForReadTrx = std::make_unique<HashIndexHeader>(
        headerArray->get(INDEX_HEADER_IDX_IN_ARRAY, TransactionType::READ_ONLY));
    this->indexHeaderForWriteTrx = std::make_unique<HashIndexHeader>(*indexHeaderForReadTrx);
    KU_ASSERT(
        this->indexHeaderForReadTrx->keyDataTypeID == TypeUtils::getPhysicalTypeIDForType<T>());
    pSlots = std::make_unique<BaseDiskArray<Slot<T>>>(*fileHandle, dbFileIDAndName.dbFileID,
        NUM_HEADER_PAGES * indexPos + P_SLOTS_HEADER_PAGE_IDX, &bm, wal,
        Transaction::getDummyReadOnlyTrx().get(), true /*bypassWAL*/);
    oSlots = std::make_unique<BaseDiskArray<Slot<T>>>(*fileHandle, dbFileIDAndName.dbFileID,
        NUM_HEADER_PAGES * indexPos + O_SLOTS_HEADER_PAGE_IDX, &bm, wal,
        Transaction::getDummyReadOnlyTrx().get(), true /*bypassWAL*/);
    // Initialize functions.
    localStorage = std::make_unique<HashIndexLocalStorage<BufferKeyType>>();
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
bool HashIndex<T>::lookupInternal(Transaction* transaction, Key key, offset_t& result) {
    if (transaction->isReadOnly()) {
        return lookupInPersistentIndex(transaction->getType(), key, result);
    } else {
        KU_ASSERT(transaction->isWriteTransaction());
        auto localLookupState = localStorage->lookup(key, result);
        if (localLookupState == HashIndexLocalLookupState::KEY_DELETED) {
            return false;
        } else if (localLookupState == HashIndexLocalLookupState::KEY_FOUND ||
                   bulkInsertLocalStorage.lookup(key, result)) {
            return true;
        } else {
            KU_ASSERT(localLookupState == HashIndexLocalLookupState::KEY_NOT_EXIST);
            return lookupInPersistentIndex(transaction->getType(), key, result);
        }
    }
}

// For deletions, we don't check if the deleted keys exist or not. Thus, we don't need to check
// in the persistent storage and directly delete keys in the local storage.
template<typename T>
void HashIndex<T>::deleteInternal(Key key) const {
    localStorage->deleteKey(key);
}

// For insertions, we first check in the local storage. There are three cases:
// - the key is found in the local storage, return false;
// - the key is marked as deleted in the local storage, insert the key to the local storage;
// - the key doesn't exist in the local storage, check if the key exists in the persistent
// index, if
//   so, return false, else insert the key to the local storage.
template<typename T>
bool HashIndex<T>::insertInternal(Key key, offset_t value) {
    offset_t tmpResult;
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
bool HashIndex<T>::lookupInPersistentIndex(TransactionType trxType, Key key, offset_t& result) {
    auto& header = trxType == TransactionType::READ_ONLY ? *this->indexHeaderForReadTrx :
                                                           *this->indexHeaderForWriteTrx;
    // There may not be any primary key slots if we try to lookup on an empty index
    if (header.numEntries == 0) {
        return false;
    }
    auto hashValue = HashIndexUtils::hash(key);
    auto fingerprint = HashIndexUtils::getFingerprintForHash(hashValue);
    auto iter =
        getSlotIterator(HashIndexUtils::getPrimarySlotIdForHash(header, hashValue), trxType);
    do {
        auto entryPos = findMatchedEntryInSlot(trxType, iter.slot, key, fingerprint);
        if (entryPos != SlotHeader::INVALID_ENTRY_POS) {
            result = iter.slot.entries[entryPos].value;
            return true;
        }
    } while (nextChainedSlot(trxType, iter));
    return false;
}

template<typename T>
void HashIndex<T>::insertIntoPersistentIndex(Key key, offset_t value) {
    auto& header = *this->indexHeaderForWriteTrx;
    slot_id_t numRequiredEntries = HashIndexUtils::getNumRequiredEntries(header.numEntries + 1);
    while (numRequiredEntries >
           pSlots->getNumElements(TransactionType::WRITE) * getSlotCapacity<T>()) {
        this->splitSlot(header);
    }
    auto hashValue = HashIndexUtils::hash(key);
    auto fingerprint = HashIndexUtils::getFingerprintForHash(hashValue);
    auto iter = getSlotIterator(HashIndexUtils::getPrimarySlotIdForHash(header, hashValue),
        TransactionType::WRITE);
    // Find a slot with free entries
    while (iter.slot.header.numEntries() == getSlotCapacity<T>() &&
           nextChainedSlot(TransactionType::WRITE, iter))
        ;
    copyKVOrEntryToSlot<Key, false /* insert kv */>(iter, key, value, fingerprint);
    header.numEntries++;
}

template<typename T>
void HashIndex<T>::deleteFromPersistentIndex(Key key) {
    auto trxType = TransactionType::WRITE;
    auto header = *this->indexHeaderForWriteTrx;
    auto hashValue = HashIndexUtils::hash(key);
    auto fingerprint = HashIndexUtils::getFingerprintForHash(hashValue);
    auto iter =
        getSlotIterator(HashIndexUtils::getPrimarySlotIdForHash(header, hashValue), trxType);
    do {
        auto entryPos = findMatchedEntryInSlot(trxType, iter.slot, key, fingerprint);
        if (entryPos != SlotHeader::INVALID_ENTRY_POS) {
            iter.slot.header.setEntryInvalid(entryPos);
            updateSlot(iter.slotInfo, iter.slot);
            header.numEntries--;
        }
    } while (nextChainedSlot(trxType, iter));
}

template<>
inline common::hash_t HashIndex<ku_string_t>::hashStored(transaction::TransactionType /*trxType*/,
    const ku_string_t& key) const {
    common::hash_t hash;
    auto str = overflowFileHandle->readString(TransactionType::WRITE, key);
    function::Hash::operation(str, hash);
    return hash;
}

template<typename T>
entry_pos_t HashIndex<T>::findMatchedEntryInSlot(TransactionType trxType, const Slot<T>& slot,
    Key key, uint8_t fingerprint) const {
    for (auto entryPos = 0u; entryPos < getSlotCapacity<T>(); entryPos++) {
        if (slot.header.isEntryValid(entryPos) &&
            slot.header.fingerprints[entryPos] == fingerprint &&
            equals(trxType, key, slot.entries[entryPos].key)) {
            return entryPos;
        }
    }
    return SlotHeader::INVALID_ENTRY_POS;
}

template<typename T>
void HashIndex<T>::prepareCommit() {
    if (localStorage->hasUpdates()) {
        wal->addToUpdatedTables(dbFileIDAndName.dbFileID.nodeIndexID.tableID);
        auto netInserts = localStorage->getNetInserts();
        if (netInserts > 0) {
            reserve(netInserts);
        }
        localStorage->applyLocalChanges(
            [this](Key key) -> void { this->deleteFromPersistentIndex(key); },
            [this](Key key, offset_t value) -> void {
                this->insertIntoPersistentIndex(key, value);
            });
        headerArray->update(INDEX_HEADER_IDX_IN_ARRAY, *indexHeaderForWriteTrx);
    }
    if (bulkInsertLocalStorage.size() > 0) {
        wal->addToUpdatedTables(dbFileIDAndName.dbFileID.nodeIndexID.tableID);
        mergeBulkInserts();
        headerArray->update(INDEX_HEADER_IDX_IN_ARRAY, *indexHeaderForWriteTrx);
    }
    headerArray->prepareCommit();
    pSlots->prepareCommit();
    oSlots->prepareCommit();
}

template<typename T>
void HashIndex<T>::prepareRollback() {
    if (localStorage->hasUpdates()) {
        wal->addToUpdatedTables(dbFileIDAndName.dbFileID.nodeIndexID.tableID);
    }
}

template<typename T>
void HashIndex<T>::checkpointInMemory() {
    if (!localStorage->hasUpdates() && bulkInsertLocalStorage.size() == 0) {
        return;
    }
    *indexHeaderForReadTrx = *indexHeaderForWriteTrx;
    headerArray->checkpointInMemoryIfNecessary();
    pSlots->checkpointInMemoryIfNecessary();
    oSlots->checkpointInMemoryIfNecessary();
    localStorage->clear();
    if constexpr (std::same_as<ku_string_t, T>) {
        overflowFileHandle->checkpointInMemory();
    }
    bulkInsertLocalStorage.clear();
}

template<typename T>
void HashIndex<T>::rollbackInMemory() {
    if (!localStorage->hasUpdates() && bulkInsertLocalStorage.size() == 0) {
        return;
    }
    headerArray->rollbackInMemoryIfNecessary();
    pSlots->rollbackInMemoryIfNecessary();
    oSlots->rollbackInMemoryIfNecessary();
    localStorage->clear();
    bulkInsertLocalStorage.clear();
    *indexHeaderForWriteTrx = *indexHeaderForReadTrx;
}

template<>
inline bool HashIndex<ku_string_t>::equals(transaction::TransactionType trxType,
    std::string_view keyToLookup, const ku_string_t& keyInEntry) const {
    if (HashIndexUtils::areStringPrefixAndLenEqual(keyToLookup, keyInEntry)) {
        auto entryKeyString = overflowFileHandle->readString(trxType, keyInEntry);
        return memcmp(keyToLookup.data(), entryKeyString.c_str(), entryKeyString.length()) == 0;
    }
    return false;
}

template<>
inline void HashIndex<ku_string_t>::insert(std::string_view key, SlotEntry<ku_string_t>& entry,
    common::offset_t offset) {
    entry.key = overflowFileHandle->writeString(key);
    entry.value = offset;
}

template<typename T>
void HashIndex<T>::splitSlot(HashIndexHeader& header) {
    appendPSlot();
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
        for (auto entryPos = 0u; entryPos < getSlotCapacity<T>(); entryPos++) {
            if (!slotHeader.isEntryValid(entryPos)) {
                continue; // Skip invalid entries.
            }
            const auto& key = slot.entries[entryPos].key;
            hash_t hash = this->hashStored(TransactionType::WRITE, key);
            auto fingerprint = HashIndexUtils::getFingerprintForHash(hash);
            auto newSlotId = hash & header.higherLevelHashMask;
            KU_ASSERT(newSlotId < pSlots->getNumElements(TransactionType::WRITE));
            copyEntryToSlot(newSlotId, key, fingerprint);
        }
    }
}

template<typename T>
std::vector<std::pair<SlotInfo, Slot<T>>> HashIndex<T>::getChainedSlots(slot_id_t pSlotId) {
    std::vector<std::pair<SlotInfo, Slot<T>>> slots;
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
void HashIndex<T>::copyEntryToSlot(slot_id_t slotId, const T& entry, uint8_t fingerprint) {
    auto iter = getSlotIterator(slotId, TransactionType::WRITE);
    do {
        if (iter.slot.header.numEntries() < getSlotCapacity<T>()) {
            // Found a slot with empty space.
            break;
        }
    } while (nextChainedSlot(TransactionType::WRITE, iter));
    copyKVOrEntryToSlot<const T&, true /* copy entry */>(iter, entry, UINT32_MAX, fingerprint);
}
template<typename T>
void HashIndex<T>::reserve(uint64_t newEntries) {
    slot_id_t numRequiredEntries = HashIndexUtils::getNumRequiredEntries(
        this->indexHeaderForWriteTrx->numEntries + newEntries);
    // Can be no fewer slots that the current level requires
    auto numRequiredSlots =
        std::max((numRequiredEntries + getSlotCapacity<T>() - 1) / getSlotCapacity<T>(),
            static_cast<slot_id_t>(1ul << this->indexHeaderForWriteTrx->currentLevel));
    // If there are no entries, we can just re-size the number of primary slots and re-calculate the
    // levels
    if (this->indexHeaderForWriteTrx->numEntries == 0) {
        pSlots->resize(numRequiredSlots);

        auto numSlotsOfCurrentLevel = 1u << this->indexHeaderForWriteTrx->currentLevel;
        while ((numSlotsOfCurrentLevel << 1) <= numRequiredSlots) {
            this->indexHeaderForWriteTrx->incrementLevel();
            numSlotsOfCurrentLevel <<= 1;
        }
        if (numRequiredSlots >= numSlotsOfCurrentLevel) {
            this->indexHeaderForWriteTrx->nextSplitSlotId =
                numRequiredSlots - numSlotsOfCurrentLevel;
        };
    } else {
        // Otherwise, split and re-hash until there are enough primary slots
        // TODO(bmwinger): resize pSlots first, update the levels and then rehash from the original
        // nextSplitSlotId to the new nextSplitSlotId using the final slot function to avoid
        // re-hashing a slot multiple times
        for (auto slots = pSlots->getNumElements(TransactionType::WRITE); slots < numRequiredSlots;
             slots++) {
            // TODO: Use diskarray iterator to avoid updating the same page repeatedly
            // appendPSlot/pushBack
            splitSlot(*this->indexHeaderForWriteTrx);
        }
    }
}

template<typename T>
void HashIndex<T>::mergeBulkInserts() {
    // TODO: Ideally we can split slots at the same time that we insert new ones
    // Compute the new number of primary slots, and iterate over each slot, determining if it
    // needs to be split (and how many times, which is complicated) and insert/rehash each element
    // one by one. Rehashed entries should be copied into a new slot in-memory, and then that new
    // slot (with the entries from the respective slot in the local storage) should be processed
    // immediately to avoid increasing memory usage (caching one page of slots at a time since split
    // slots usually get rehashed to a new page).
    //
    // On the other hand, two passes may not be significantly slower than one
    // TODO: one pass would also reduce locking when frames are unpinned,
    // which is useful if this can be paralellized
    reserve(bulkInsertLocalStorage.size());
    KU_ASSERT(
        bulkInsertLocalStorage.numPrimarySlots() == pSlots->getNumElements(TransactionType::WRITE));
    KU_ASSERT(this->indexHeaderForWriteTrx->currentLevel ==
              bulkInsertLocalStorage.getIndexHeader().currentLevel);
    KU_ASSERT(this->indexHeaderForWriteTrx->levelHashMask ==
              bulkInsertLocalStorage.getIndexHeader().levelHashMask);
    KU_ASSERT(this->indexHeaderForWriteTrx->higherLevelHashMask ==
              bulkInsertLocalStorage.getIndexHeader().higherLevelHashMask);
    KU_ASSERT(this->indexHeaderForWriteTrx->nextSplitSlotId ==
              bulkInsertLocalStorage.getIndexHeader().nextSplitSlotId);

    auto originalNumEntries = this->indexHeaderForWriteTrx->numEntries;

    // Storing as many slots in-memory as on-disk shouldn't be necessary (for one it makes memory
    // usage an issue as we may need significantly more memory to store the slots that we would
    // otherwise) Instead, when merging here we can re-hash and split each in-memory slot (into
    // temporary vector buffers instead of slots for improved performance) and then merge each of
    // those one at a time into the disk slots. That will keep the low memory requirements and still
    // let us update each on-disk slot one at a time.

    auto diskSlotIterator = pSlots->iter_mut();
    // TODO: Use a separate random access iterator and one that's sequential for adding new overflow
    // slots All new slots will be sequential and benefit from caching, but for existing randomly
    // accessed slots we just benefit from the interface. However, the two iterators would not be
    // able to pin the same page simultaneously
    auto diskOverflowSlotIterator = oSlots->iter_mut();
    for (uint64_t slotId = 0; slotId < bulkInsertLocalStorage.numPrimarySlots(); slotId++) {
        auto localSlot = typename InMemHashIndex<T>::SlotIterator(slotId, &bulkInsertLocalStorage);
        // If mask is empty, skip this slot
        if (!localSlot.slot->header.validityMask) {
            // There should be no entries in the overflow slots, as we never leave gaps in the
            // builder
            continue;
        }
        diskSlotIterator.seek(slotId);
        mergeSlot(localSlot, diskSlotIterator, diskOverflowSlotIterator, slotId);
    }
    KU_ASSERT(originalNumEntries + bulkInsertLocalStorage.getIndexHeader().numEntries ==
              indexHeaderForWriteTrx->numEntries);
}

template<typename T>
void HashIndex<T>::mergeSlot(typename InMemHashIndex<T>::SlotIterator& slotToMerge,
    typename BaseDiskArray<Slot<T>>::WriteIterator& diskSlotIterator,
    typename BaseDiskArray<Slot<T>>::WriteIterator& diskOverflowSlotIterator, slot_id_t slotId) {
    slot_id_t diskEntryPos = 0u;
    auto* diskSlot = &*diskSlotIterator;
    // Merge slot from local storage to existing slot
    do {
        for (auto entryPos = 0u; entryPos < slotToMerge.slot->header.numEntries(); entryPos++) {
            KU_ASSERT(slotToMerge.slot->header.isEntryValid(entryPos));
            // Find the next empty entry, or add a new slot if there are no more entries
            while (diskSlot->header.isEntryValid(diskEntryPos) ||
                   diskEntryPos >= getSlotCapacity<T>()) {
                diskEntryPos++;
                if (diskEntryPos >= getSlotCapacity<T>()) {
                    if (diskSlot->header.nextOvfSlotId == 0) {
                        // If there are no more disk slots in this chain, we need to add one
                        diskSlot->header.nextOvfSlotId = diskOverflowSlotIterator.size();
                        // This may invalidate diskSlot
                        diskOverflowSlotIterator.pushBack(Slot<T>());
                    } else {
                        diskOverflowSlotIterator.seek(diskSlot->header.nextOvfSlotId);
                    }
                    diskSlot = &*diskOverflowSlotIterator;
                    // Check to make sure we're not looping
                    KU_ASSERT(diskOverflowSlotIterator.idx() != diskSlot->header.nextOvfSlotId);
                    diskEntryPos = 0;
                }
            }
            KU_ASSERT(diskEntryPos < getSlotCapacity<T>());
            copyAndUpdateSlotHeader<const T&, true>(*diskSlot, diskEntryPos,
                slotToMerge.slot->entries[entryPos].key, UINT32_MAX,
                slotToMerge.slot->header.fingerprints[entryPos]);
            KU_ASSERT([&]() {
                auto hash =
                    hashStored(TransactionType::WRITE, slotToMerge.slot->entries[entryPos].key);
                auto primarySlot =
                    HashIndexUtils::getPrimarySlotIdForHash(*indexHeaderForWriteTrx, hash);
                KU_ASSERT(slotToMerge.slot->header.fingerprints[entryPos] ==
                          HashIndexUtils::getFingerprintForHash(hash));
                KU_ASSERT(primarySlot == slotId);
                return true;
            }());
            indexHeaderForWriteTrx->numEntries++;
            diskEntryPos++;
        }
    } while (bulkInsertLocalStorage.nextChainedSlot(slotToMerge));
}

template<typename T>
HashIndex<T>::~HashIndex() = default;

template class HashIndex<int64_t>;
template class HashIndex<int32_t>;
template class HashIndex<int16_t>;
template class HashIndex<int8_t>;
template class HashIndex<uint64_t>;
template class HashIndex<uint32_t>;
template class HashIndex<uint16_t>;
template class HashIndex<uint8_t>;
template class HashIndex<double>;
template class HashIndex<float>;
template class HashIndex<int128_t>;
template class HashIndex<ku_string_t>;

PrimaryKeyIndex::PrimaryKeyIndex(const DBFileIDAndName& dbFileIDAndName, bool readOnly,
    common::PhysicalTypeID keyDataType, BufferManager& bufferManager, WAL* wal,
    VirtualFileSystem* vfs)
    : hasRunPrepareCommit{false}, keyDataTypeID(keyDataType), bufferManager{bufferManager} {
    fileHandle = bufferManager.getBMFileHandle(dbFileIDAndName.fName,
        readOnly ? FileHandle::O_PERSISTENT_FILE_READ_ONLY :
                   FileHandle::O_PERSISTENT_FILE_NO_CREATE,
        BMFileHandle::FileVersionedType::VERSIONED_FILE, vfs);
    if (keyDataTypeID == PhysicalTypeID::STRING) {
        overflowFile =
            std::make_unique<OverflowFile>(dbFileIDAndName, &bufferManager, wal, readOnly, vfs);
    }

    hashIndices.reserve(NUM_HASH_INDEXES);
    TypeUtils::visit(
        keyDataTypeID,
        [&](ku_string_t) {
            for (auto i = 0u; i < NUM_HASH_INDEXES; i++) {
                hashIndices.push_back(std::make_unique<HashIndex<ku_string_t>>(dbFileIDAndName,
                    fileHandle, overflowFile->addHandle(), i, bufferManager, wal));
            }
        },
        [&]<HashablePrimitive T>(T) {
            for (auto i = 0u; i < NUM_HASH_INDEXES; i++) {
                hashIndices.push_back(std::make_unique<HashIndex<T>>(dbFileIDAndName, fileHandle,
                    nullptr, i, bufferManager, wal));
            }
        },
        [&](auto) { KU_UNREACHABLE; });
}

bool PrimaryKeyIndex::lookup(Transaction* trx, common::ValueVector* keyVector, uint64_t vectorPos,
    common::offset_t& result) {
    bool retVal = false;
    TypeUtils::visit(
        keyDataTypeID,
        [&]<IndexHashable T>(T) {
            T key = keyVector->getValue<T>(vectorPos);
            retVal = lookup(trx, key, result);
        },
        [](auto) { KU_UNREACHABLE; });
    return retVal;
}

bool PrimaryKeyIndex::insert(common::ValueVector* keyVector, uint64_t vectorPos,
    common::offset_t value) {
    bool result = false;
    TypeUtils::visit(
        keyDataTypeID,
        [&]<IndexHashable T>(T) {
            T key = keyVector->getValue<T>(vectorPos);
            result = insert(key, value);
        },
        [](auto) { KU_UNREACHABLE; });
    return result;
}

void PrimaryKeyIndex::delete_(ValueVector* keyVector) {
    TypeUtils::visit(
        keyDataTypeID,
        [&]<IndexHashable T>(T) {
            for (auto i = 0u; i < keyVector->state->selVector->selectedSize; i++) {
                auto pos = keyVector->state->selVector->selectedPositions[i];
                if (keyVector->isNull(pos)) {
                    continue;
                }
                auto key = keyVector->getValue<T>(pos);
                delete_(key);
            }
        },
        [](auto) { KU_UNREACHABLE; });
}

void PrimaryKeyIndex::checkpointInMemory() {
    for (auto i = 0u; i < NUM_HASH_INDEXES; i++) {
        hashIndices[i]->checkpointInMemory();
    }
    if (overflowFile) {
        overflowFile->checkpointInMemory();
    }
    hasRunPrepareCommit = false;
}

void PrimaryKeyIndex::rollbackInMemory() {
    for (auto i = 0u; i < NUM_HASH_INDEXES; i++) {
        hashIndices[i]->rollbackInMemory();
    }
    if (overflowFile) {
        overflowFile->rollbackInMemory();
    }
    hasRunPrepareCommit = false;
}

void PrimaryKeyIndex::prepareCommit() {
    if (!hasRunPrepareCommit) {
        for (auto i = 0u; i < NUM_HASH_INDEXES; i++) {
            hashIndices[i]->prepareCommit();
        }
        if (overflowFile) {
            overflowFile->prepareCommit();
        }
        // Make sure that changes which bypassed the WAL are written.
        // There is no other mechanism for enforcing that they are flushed
        // and they will be dropped when the file handle is destroyed.
        // TODO: Should eventually be moved into the disk array when the disk array can
        // generally handle bypassing the WAL, but should only be run once per file, not once per
        // disk array
        bufferManager.flushAllDirtyPagesInFrames(*fileHandle);
        hasRunPrepareCommit = true;
    }
}

void PrimaryKeyIndex::prepareRollback() {
    for (auto i = 0u; i < NUM_HASH_INDEXES; i++) {
        hashIndices[i]->prepareRollback();
    }
}

PrimaryKeyIndex::~PrimaryKeyIndex() = default;

void PrimaryKeyIndex::createEmptyHashIndexFiles(PhysicalTypeID typeID, const std::string& fName,
    VirtualFileSystem* vfs) {
    FileHandle fileHandle(fName, FileHandle::O_PERSISTENT_FILE_CREATE_NOT_EXISTS, vfs);
    fileHandle.addNewPages(NUM_HEADER_PAGES * NUM_HASH_INDEXES);
    common::TypeUtils::visit(
        typeID,
        [&]<common::IndexHashable T>(T) {
            if constexpr (std::same_as<T, common::ku_string_t>) {
                OverflowFile::createEmptyFiles(StorageUtils::getOverflowFileName(fName), vfs);
            }
            for (auto i = 0u; i < NUM_HASH_INDEXES; i++) {
                InMemHashIndex<T>::createEmptyIndexFiles(i, fileHandle);
            }
        },
        [&](auto) { KU_UNREACHABLE; });
}

} // namespace storage
} // namespace kuzu
