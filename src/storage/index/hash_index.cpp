#include "storage/index/hash_index.h"

#include <bitset>
#include <cstdint>
#include <type_traits>

#include "common/assert.h"
#include "common/constants.h"
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
#include "storage/storage_structure/overflow_file.h"
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
    explicit HashIndexLocalStorage(OverflowFileHandle* handle)
        : localDeletions{}, localInsertions{handle} {}
    using OwnedKeyType =
        typename std::conditional<std::same_as<T, common::ku_string_t>, std::string, T>::type;
    using Key = typename std::conditional<std::same_as<T, ku_string_t>, std::string_view, T>::type;
    HashIndexLocalLookupState lookup(Key key, common::offset_t& result) {
        if (localDeletions.contains(key)) {
            return HashIndexLocalLookupState::KEY_DELETED;
        }
        if (localInsertions.lookup(key, result)) {
            return HashIndexLocalLookupState::KEY_FOUND;
        } else {
            return HashIndexLocalLookupState::KEY_NOT_EXIST;
        }
    }

    void deleteKey(Key key) {
        if (!localInsertions.deleteKey(key)) {
            localDeletions.insert(static_cast<OwnedKeyType>(key));
        }
    }

    bool insert(Key key, common::offset_t value) {
        auto iter = localDeletions.find(key);
        if (iter != localDeletions.end()) {
            localDeletions.erase(iter);
        }
        return localInsertions.append(key, value);
    }

    size_t append(const IndexBuffer<OwnedKeyType>& buffer) {
        return localInsertions.append(buffer);
    }

    inline bool hasUpdates() const { return !(localInsertions.empty() && localDeletions.empty()); }

    inline int64_t getNetInserts() const {
        return static_cast<int64_t>(localInsertions.size()) - localDeletions.size();
    }

    inline void clear() {
        localInsertions.clear();
        localDeletions.clear();
    }

    void applyLocalChanges(const std::function<void(Key)>& deleteOp,
        const std::function<void(const InMemHashIndex<T>&)>& insertOp) {
        for (auto& key : localDeletions) {
            deleteOp(key);
        }
        insertOp(localInsertions);
    }

    void reserveInserts(uint64_t newEntries) { localInsertions.reserve(newEntries); }

    const InMemHashIndex<T>& getInsertions() { return localInsertions; }

private:
    // When the storage type is string, allow the key type to be string_view with a custom hash
    // function
    using hash_function = typename std::conditional<std::is_same<OwnedKeyType, std::string>::value,
        common::StringUtils::string_hash, std::hash<T>>::type;
    std::unordered_set<OwnedKeyType, hash_function, std::equal_to<>> localDeletions;
    InMemHashIndex<T> localInsertions;
};

template<typename T>
HashIndex<T>::HashIndex(const DBFileIDAndName& dbFileIDAndName,
    const std::shared_ptr<BMFileHandle>& fileHandle, OverflowFileHandle* overflowFileHandle,
    uint64_t indexPos, BufferManager& bufferManager, WAL* wal)
    : dbFileIDAndName{dbFileIDAndName}, bm{bufferManager}, wal{wal},
      headerPageIdx{NUM_HEADER_PAGES * indexPos + INDEX_HEADER_ARRAY_HEADER_PAGE_IDX},
      fileHandle(fileHandle), overflowFileHandle(overflowFileHandle) {
    this->indexHeaderForReadTrx = std::make_unique<HashIndexHeader>();
    bufferManager.optimisticRead(*fileHandle, headerPageIdx, [&](auto* frame) {
        memcpy(this->indexHeaderForReadTrx.get(), frame, sizeof(HashIndexHeader));
    });
    // Read indexHeader from the headerArray, which contains only one element.
    this->indexHeaderForWriteTrx = std::make_unique<HashIndexHeader>(*indexHeaderForReadTrx);
    KU_ASSERT(
        this->indexHeaderForReadTrx->keyDataTypeID == TypeUtils::getPhysicalTypeIDForType<T>());
    pSlots = std::make_unique<DiskArray<Slot<T>>>(*fileHandle, dbFileIDAndName.dbFileID,
        NUM_HEADER_PAGES * indexPos + P_SLOTS_HEADER_PAGE_IDX, &bm, wal,
        Transaction::getDummyReadOnlyTrx().get(), true /*bypassWAL*/);
    oSlots = std::make_unique<DiskArray<Slot<T>>>(*fileHandle, dbFileIDAndName.dbFileID,
        NUM_HEADER_PAGES * indexPos + O_SLOTS_HEADER_PAGE_IDX, &bm, wal,
        Transaction::getDummyReadOnlyTrx().get(), true /*bypassWAL*/);
    // Initialize functions.
    localStorage = std::make_unique<HashIndexLocalStorage<T>>(overflowFileHandle);
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
        } else if (localLookupState == HashIndexLocalLookupState::KEY_FOUND) {
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
size_t HashIndex<T>::append(const IndexBuffer<BufferKeyType>& buffer) {
    // Check if values already exist in persistent storage
    if (indexHeaderForWriteTrx->numEntries > 0) {
        common::offset_t result;
        for (size_t i = 0; i < buffer.size(); i++) {
            const auto& [key, value] = buffer[i];
            if (lookupInPersistentIndex(transaction::TransactionType::WRITE, key, result)) {
                return i;
            }
        }
    }
    return localStorage->append(buffer);
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
    reserve(1);
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
    if (header.numEntries == 0) {
        return;
    }
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
        localStorage->applyLocalChanges([&](Key key) { deleteFromPersistentIndex(key); },
            [&](const auto& insertions) { mergeBulkInserts(insertions); });
        DBFileUtils::updatePage(*fileHandle, dbFileIDAndName.dbFileID, headerPageIdx,
            true /*don't need to read original data*/, bm, *wal, [&](auto* frame) {
                memcpy(frame, indexHeaderForWriteTrx.get(), sizeof(HashIndexHeader));
            });
    }
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
    if (!localStorage->hasUpdates()) {
        return;
    }
    *indexHeaderForReadTrx = *indexHeaderForWriteTrx;
    pSlots->checkpointInMemoryIfNecessary();
    oSlots->checkpointInMemoryIfNecessary();
    localStorage->clear();
    if constexpr (std::same_as<ku_string_t, T>) {
        overflowFileHandle->checkpointInMemory();
    }
}

template<typename T>
void HashIndex<T>::rollbackInMemory() {
    if (!localStorage->hasUpdates()) {
        return;
    }
    pSlots->rollbackInMemoryIfNecessary();
    oSlots->rollbackInMemoryIfNecessary();
    localStorage->clear();
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
void HashIndex<T>::splitSlots(HashIndexHeader& header, slot_id_t numSlotsToSplit) {
    auto originalSlotIterator = pSlots->iter_mut();
    auto newSlotIterator = pSlots->iter_mut();
    auto overflowSlotIterator = oSlots->iter_mut();
    // The overflow slot iterators will hang if they access the same page
    // So instead buffer new overflow slots here and append them at the end
    std::vector<Slot<T>> newOverflowSlots;

    for (slot_id_t i = 0; i < numSlotsToSplit; i++) {
        auto* newSlot = &*newSlotIterator.pushBack(Slot<T>());
        entry_pos_t newEntryPos = 0;
        Slot<T>* originalSlot = &*originalSlotIterator.seek(header.nextSplitSlotId);
        do {
            for (entry_pos_t originalEntryPos = 0; originalEntryPos < getSlotCapacity<T>();
                 originalEntryPos++) {
                if (!originalSlot->header.isEntryValid(originalEntryPos)) {
                    continue; // Skip invalid entries.
                }
                if (newEntryPos >= getSlotCapacity<T>()) {
                    newOverflowSlots.emplace_back();
                    newSlot = &newOverflowSlots.back();
                    newEntryPos = 0;
                }
                // Copy entry from old slot to new slot
                const auto& key = originalSlot->entries[originalEntryPos].key;
                hash_t hash = this->hashStored(TransactionType::WRITE, key);
                auto newSlotId = hash & header.higherLevelHashMask;
                if (newSlotId != header.nextSplitSlotId) {
                    KU_ASSERT(newSlotId == newSlotIterator.idx());
                    copyAndUpdateSlotHeader<const T&, true>(*newSlot, newEntryPos,
                        originalSlot->entries[originalEntryPos].key, UINT32_MAX,
                        originalSlot->header.fingerprints[originalEntryPos]);
                    originalSlot->header.setEntryInvalid(originalEntryPos);
                    newEntryPos++;
                }
            }
        } while (originalSlot->header.nextOvfSlotId != SlotHeader::INVALID_OVERFLOW_SLOT_ID &&
                 (originalSlot = &*overflowSlotIterator.seek(originalSlot->header.nextOvfSlotId)));
        header.incrementNextSplitSlotId();
    }
    for (auto&& slot : newOverflowSlots) {
        overflowSlotIterator.pushBack(std::move(slot));
    }
}

template<typename T>
std::vector<std::pair<SlotInfo, Slot<T>>> HashIndex<T>::getChainedSlots(slot_id_t pSlotId) {
    std::vector<std::pair<SlotInfo, Slot<T>>> slots;
    SlotInfo slotInfo{pSlotId, SlotType::PRIMARY};
    while (slotInfo.slotType == SlotType::PRIMARY ||
           slotInfo.slotId != SlotHeader::INVALID_OVERFLOW_SLOT_ID) {
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
    // Always start with at least one page worth of slots.
    // This guarantees that when splitting the source and destination slot are never on the same
    // page
    // Which allows safe use of multiple disk array iterators.
    numRequiredSlots = std::max(numRequiredSlots,
        BufferPoolConstants::PAGE_4KB_SIZE / pSlots->getAlignedElementSize());
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
        splitSlots(*this->indexHeaderForWriteTrx,
            numRequiredSlots - pSlots->getNumElements(TransactionType::WRITE));
    }
}

template<typename T>
void HashIndex<T>::sortEntries(const InMemHashIndex<T>& insertLocalStorage,
    typename InMemHashIndex<T>::SlotIterator& slotToMerge,
    std::vector<HashIndexEntryView>& entries) {
    do {
        auto numEntries = slotToMerge.slot->header.numEntries();
        for (auto entryPos = 0u; entryPos < numEntries; entryPos++) {
            const auto* entry = &slotToMerge.slot->entries[entryPos];
            const auto hash = hashStored(TransactionType::WRITE, entry->key);
            const auto primarySlot =
                HashIndexUtils::getPrimarySlotIdForHash(*indexHeaderForWriteTrx, hash);
            entries.push_back(HashIndexEntryView{primarySlot,
                slotToMerge.slot->header.fingerprints[entryPos], entry});
        }
    } while (insertLocalStorage.nextChainedSlot(slotToMerge));
    std::sort(entries.begin(), entries.end(), [&](auto entry1, auto entry2) -> bool {
        // Sort based on the entry's disk slot ID so that the first slot is at the end
        // Sorting is done reversed so that we can process from the back of the list,
        // using the size to track the remaining entries
        return entry1.diskSlotId > entry2.diskSlotId;
    });
}

template<typename T>
void HashIndex<T>::mergeBulkInserts(const InMemHashIndex<T>& insertLocalStorage) {
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
    // which is useful if this can be parallelized
    reserve(insertLocalStorage.size());
    RUNTIME_CHECK(auto originalNumEntries = this->indexHeaderForWriteTrx->numEntries;);

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
    // Alternatively, cache new slots in memory and pushBack them at the end like in splitSlots
    auto diskOverflowSlotIterator = oSlots->iter_mut();

    // Store sorted slot positions. Re-use to avoid re-allocating memory
    // TODO: Unify implementations to make sure this matches the size used by the disk array
    constexpr size_t NUM_SLOTS_PER_PAGE =
        BufferPoolConstants::PAGE_4KB_SIZE / DiskArray<Slot<T>>::getAlignedElementSize();
    std::array<std::vector<HashIndexEntryView>, NUM_SLOTS_PER_PAGE> partitionedEntries;
    // Sort entries for a page of slots at a time, then move vertically and process all entries
    // which map to a given page on disk, then horizontally to the next page in the set. These pages
    // may not be consecutive, but we reduce the memory overhead for storing the information about
    // the sorted data and still just process each page once.
    for (uint64_t localSlotId = 0; localSlotId < insertLocalStorage.numPrimarySlots();
         localSlotId += NUM_SLOTS_PER_PAGE) {
        for (size_t i = 0;
             i < NUM_SLOTS_PER_PAGE && localSlotId + i < insertLocalStorage.numPrimarySlots();
             i++) {
            auto localSlot =
                typename InMemHashIndex<T>::SlotIterator(localSlotId + i, &insertLocalStorage);
            partitionedEntries[i].clear();
            // Results are sorted in reverse, so we can process the end first and pop_back to remove
            // them from the vector
            sortEntries(insertLocalStorage, localSlot, partitionedEntries[i]);
        }
        // Repeat until there are no un-processed partitions in partitionedEntries
        // This will run at most NUM_SLOTS_PER_PAGE times the number of entries
        std::bitset<NUM_SLOTS_PER_PAGE> done;
        while (!done.all()) {
            std::optional<page_idx_t> diskSlotPage;
            for (size_t i = 0; i < NUM_SLOTS_PER_PAGE; i++) {
                if (!done[i] && !partitionedEntries[i].empty()) {
                    auto diskSlotId = partitionedEntries[i].back().diskSlotId;
                    if (!diskSlotPage) {
                        diskSlotPage = diskSlotId / NUM_SLOTS_PER_PAGE;
                    }
                    if (diskSlotId / NUM_SLOTS_PER_PAGE == diskSlotPage) {
                        auto merged = mergeSlot(partitionedEntries[i], diskSlotIterator,
                            diskOverflowSlotIterator, diskSlotId);
                        KU_ASSERT(merged <= partitionedEntries[i].size());
                        partitionedEntries[i].resize(partitionedEntries[i].size() - merged);
                        if (partitionedEntries[i].empty()) {
                            done[i] = true;
                        }
                    }
                } else {
                    done[i] = true;
                }
            }
        }
    }
    KU_ASSERT(originalNumEntries + insertLocalStorage.getIndexHeader().numEntries ==
              indexHeaderForWriteTrx->numEntries);
}

template<typename T>
size_t HashIndex<T>::mergeSlot(const std::vector<HashIndexEntryView>& slotToMerge,
    typename DiskArray<Slot<T>>::WriteIterator& diskSlotIterator,
    typename DiskArray<Slot<T>>::WriteIterator& diskOverflowSlotIterator, slot_id_t diskSlotId) {
    slot_id_t diskEntryPos = 0u;
    // mergeSlot should only be called when there is at least one entry for the given disk slot id
    // in the slot to merge
    Slot<T>* diskSlot = &*diskSlotIterator.seek(diskSlotId);
    // Merge slot from local storage to existing slot
    size_t merged = 0;
    for (auto it = std::rbegin(slotToMerge); it != std::rend(slotToMerge); ++it) {
        if (it->diskSlotId != diskSlotId) {
            return merged;
        }
        // Find the next empty entry, or add a new slot if there are no more entries
        while (
            diskSlot->header.isEntryValid(diskEntryPos) || diskEntryPos >= getSlotCapacity<T>()) {
            diskEntryPos++;
            if (diskEntryPos >= getSlotCapacity<T>()) {
                if (diskSlot->header.nextOvfSlotId == SlotHeader::INVALID_OVERFLOW_SLOT_ID) {
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
        copyAndUpdateSlotHeader<const T&, true>(*diskSlot, diskEntryPos, it->entry->key, UINT32_MAX,
            it->fingerprint);
        KU_ASSERT([&]() {
            auto key = it->entry->key;
            auto hash = hashStored(TransactionType::WRITE, key);
            auto primarySlot =
                HashIndexUtils::getPrimarySlotIdForHash(*indexHeaderForWriteTrx, hash);
            KU_ASSERT(it->fingerprint == HashIndexUtils::getFingerprintForHash(hash));
            KU_ASSERT(primarySlot == diskSlotId);
            return true;
        }());
        indexHeaderForWriteTrx->numEntries++;
        diskEntryPos++;
        merged++;
    }
    return merged;
}

template<typename T>
void HashIndex<T>::bulkReserve(uint64_t newEntries) {
    return localStorage->reserveInserts(newEntries);
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
            for (auto i = 0u; i < keyVector->state->getSelVector().getSelSize(); i++) {
                auto pos = keyVector->state->getSelVector()[i];
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
