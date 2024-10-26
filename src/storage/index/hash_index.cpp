#include "storage/index/hash_index.h"

#include <bitset>
#include <cstdint>

#include "common/assert.h"
#include "common/constants.h"
#include "common/types/int128_t.h"
#include "common/types/ku_string.h"
#include "common/types/types.h"
#include "storage/buffer_manager/buffer_manager.h"
#include "storage/file_handle.h"
#include "storage/index/hash_index_header.h"
#include "storage/index/hash_index_slot.h"
#include "storage/index/hash_index_utils.h"
#include "storage/index/in_mem_hash_index.h"
#include "storage/local_storage/local_hash_index.h"
#include "storage/shadow_utils.h"
#include "storage/storage_structure/disk_array.h"
#include "storage/storage_structure/disk_array_collection.h"
#include "storage/storage_structure/overflow_file.h"
#include "storage/storage_utils.h"
#include "transaction/transaction.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

template<typename T>
HashIndex<T>::HashIndex(const DBFileIDAndName& dbFileIDAndName, FileHandle* fileHandle,
    OverflowFileHandle* overflowFileHandle, DiskArrayCollection& diskArrays, uint64_t indexPos,
    ShadowFile* shadowFile, const HashIndexHeader& headerForReadTrx,
    HashIndexHeader& headerForWriteTrx)
    : dbFileIDAndName{dbFileIDAndName}, shadowFile{shadowFile}, headerPageIdx(0),
      fileHandle(fileHandle), overflowFileHandle(overflowFileHandle),
      localStorage{std::make_unique<HashIndexLocalStorage<T>>(overflowFileHandle)},
      indexHeaderForReadTrx{headerForReadTrx}, indexHeaderForWriteTrx{headerForWriteTrx} {
    pSlots = diskArrays.getDiskArray<Slot<T>>(indexPos);
    oSlots = diskArrays.getDiskArray<Slot<T>>(NUM_HASH_INDEXES + indexPos);
}

template<typename T>
void HashIndex<T>::deleteFromPersistentIndex(const Transaction* transaction, Key key,
    visible_func isVisible) {
    auto& header = this->indexHeaderForWriteTrx;
    if (header.numEntries == 0) {
        return;
    }
    auto hashValue = HashIndexUtils::hash(key);
    auto fingerprint = HashIndexUtils::getFingerprintForHash(hashValue);
    auto iter =
        getSlotIterator(HashIndexUtils::getPrimarySlotIdForHash(header, hashValue), transaction);
    do {
        auto entryPos = findMatchedEntryInSlot(transaction, iter.slot, key, fingerprint, isVisible);
        if (entryPos != SlotHeader::INVALID_ENTRY_POS) {
            iter.slot.header.setEntryInvalid(entryPos);
            updateSlot(transaction, iter.slotInfo, iter.slot);
            header.numEntries--;
        }
    } while (nextChainedSlot(transaction, iter));
}

template<>
inline hash_t HashIndex<ku_string_t>::hashStored(const Transaction* transaction,
    const ku_string_t& key) const {
    hash_t hash = 0;
    auto str = overflowFileHandle->readString(transaction->getType(), key);
    function::Hash::operation(str, hash);
    return hash;
}

template<typename T>
bool HashIndex<T>::checkpoint() {
    if (localStorage->hasUpdates()) {
        auto transaction = &DUMMY_CHECKPOINT_TRANSACTION;
        auto netInserts = localStorage->getNetInserts();
        if (netInserts > 0) {
            reserve(transaction, netInserts);
        }
        localStorage->applyLocalChanges(
            [&](Key) {
                // TODO(Guodong/Ben): FIX-ME. We should vaccum the index during checkpoint.
                // DO NOTHING.
            },
            [&](const auto& insertions) { mergeBulkInserts(transaction, insertions); });
        pSlots->checkpoint();
        oSlots->checkpoint();
        return true;
    }
    pSlots->checkpoint();
    oSlots->checkpoint();
    return false;
}

template<typename T>
bool HashIndex<T>::checkpointInMemory() {
    if (!localStorage->hasUpdates()) {
        return false;
    }
    pSlots->checkpointInMemoryIfNecessary();
    oSlots->checkpointInMemoryIfNecessary();
    localStorage->clear();
    if constexpr (std::same_as<ku_string_t, T>) {
        overflowFileHandle->checkpointInMemory();
    }
    return true;
}

template<typename T>
bool HashIndex<T>::rollbackInMemory() {
    if (!localStorage->hasUpdates()) {
        return false;
    }
    pSlots->rollbackInMemoryIfNecessary();
    oSlots->rollbackInMemoryIfNecessary();
    localStorage->clear();
    return true;
}

template<typename T>
void HashIndex<T>::splitSlots(const Transaction* transaction, HashIndexHeader& header,
    slot_id_t numSlotsToSplit) {
    auto originalSlotIterator = pSlots->iter_mut();
    auto newSlotIterator = pSlots->iter_mut();
    auto overflowSlotIterator = oSlots->iter_mut();
    // The overflow slot iterators will hang if they access the same page
    // So instead buffer new overflow slots here and append them at the end
    std::vector<Slot<T>> newOverflowSlots;

    auto getNextOvfSlot = [&](slot_id_t nextOvfSlotId) {
        if (nextOvfSlotId >= oSlots->getNumElements()) {
            return &newOverflowSlots[nextOvfSlotId - oSlots->getNumElements()];
        } else {
            return &*overflowSlotIterator.seek(nextOvfSlotId);
        }
    };

    for (slot_id_t i = 0; i < numSlotsToSplit; i++) {
        auto* newSlot = &*newSlotIterator.pushBack(transaction, Slot<T>());
        entry_pos_t newEntryPos = 0;
        Slot<T>* originalSlot = &*originalSlotIterator.seek(header.nextSplitSlotId);
        do {
            for (entry_pos_t originalEntryPos = 0; originalEntryPos < getSlotCapacity<T>();
                 originalEntryPos++) {
                if (!originalSlot->header.isEntryValid(originalEntryPos)) {
                    continue; // Skip invalid entries.
                }
                if (newEntryPos >= getSlotCapacity<T>()) {
                    newSlot->header.nextOvfSlotId =
                        newOverflowSlots.size() + oSlots->getNumElements();
                    newOverflowSlots.emplace_back();
                    newSlot = &newOverflowSlots.back();
                    newEntryPos = 0;
                }
                // Copy entry from old slot to new slot
                const auto& key = originalSlot->entries[originalEntryPos].key;
                const hash_t hash = this->hashStored(transaction, key);
                const auto newSlotId = hash & header.higherLevelHashMask;
                if (newSlotId != header.nextSplitSlotId) {
                    KU_ASSERT(newSlotId == newSlotIterator.idx());
                    newSlot->entries[newEntryPos] = originalSlot->entries[originalEntryPos];
                    newSlot->header.setEntryValid(newEntryPos,
                        originalSlot->header.fingerprints[originalEntryPos]);
                    originalSlot->header.setEntryInvalid(originalEntryPos);
                    newEntryPos++;
                }
            }
        } while (originalSlot->header.nextOvfSlotId != SlotHeader::INVALID_OVERFLOW_SLOT_ID &&
                 (originalSlot = getNextOvfSlot(originalSlot->header.nextOvfSlotId)));
        header.incrementNextSplitSlotId();
    }
    for (auto&& slot : newOverflowSlots) {
        overflowSlotIterator.pushBack(transaction, std::move(slot));
    }
}

template<typename T>
std::vector<std::pair<SlotInfo, Slot<T>>> HashIndex<T>::getChainedSlots(
    const Transaction* transaction, slot_id_t pSlotId) {
    std::vector<std::pair<SlotInfo, Slot<T>>> slots;
    SlotInfo slotInfo{pSlotId, SlotType::PRIMARY};
    while (slotInfo.slotType == SlotType::PRIMARY ||
           slotInfo.slotId != SlotHeader::INVALID_OVERFLOW_SLOT_ID) {
        auto slot = getSlot(transaction, slotInfo);
        slots.emplace_back(slotInfo, slot);
        slotInfo.slotId = slot.header.nextOvfSlotId;
        slotInfo.slotType = SlotType::OVF;
    }
    return slots;
}

template<typename T>
void HashIndex<T>::reserve(const Transaction* transaction, uint64_t newEntries) {
    slot_id_t numRequiredEntries =
        HashIndexUtils::getNumRequiredEntries(this->indexHeaderForWriteTrx.numEntries + newEntries);
    // Can be no fewer slots that the current level requires
    auto numRequiredSlots =
        std::max((numRequiredEntries + getSlotCapacity<T>() - 1) / getSlotCapacity<T>(),
            static_cast<slot_id_t>(1ul << this->indexHeaderForWriteTrx.currentLevel));
    // Always start with at least one page worth of slots.
    // This guarantees that when splitting the source and destination slot are never on the same
    // page
    // Which allows safe use of multiple disk array iterators.
    numRequiredSlots = std::max(numRequiredSlots, KUZU_PAGE_SIZE / pSlots->getAlignedElementSize());
    // If there are no entries, we can just re-size the number of primary slots and re-calculate the
    // levels
    if (this->indexHeaderForWriteTrx.numEntries == 0) {
        pSlots->resize(transaction, numRequiredSlots);

        auto numSlotsOfCurrentLevel = 1u << this->indexHeaderForWriteTrx.currentLevel;
        while ((numSlotsOfCurrentLevel << 1) <= numRequiredSlots) {
            this->indexHeaderForWriteTrx.incrementLevel();
            numSlotsOfCurrentLevel <<= 1;
        }
        if (numRequiredSlots >= numSlotsOfCurrentLevel) {
            this->indexHeaderForWriteTrx.nextSplitSlotId =
                numRequiredSlots - numSlotsOfCurrentLevel;
        }
    } else {
        splitSlots(transaction, this->indexHeaderForWriteTrx,
            numRequiredSlots - pSlots->getNumElements(transaction->getType()));
    }
}

template<typename T>
void HashIndex<T>::sortEntries(const Transaction* transaction,
    const InMemHashIndex<T>& insertLocalStorage,
    typename InMemHashIndex<T>::SlotIterator& slotToMerge,
    std::vector<HashIndexEntryView>& entries) {
    do {
        auto numEntries = slotToMerge.slot->header.numEntries();
        for (auto entryPos = 0u; entryPos < numEntries; entryPos++) {
            const auto* entry = &slotToMerge.slot->entries[entryPos];
            const auto hash = hashStored(transaction, entry->key);
            const auto primarySlot =
                HashIndexUtils::getPrimarySlotIdForHash(indexHeaderForWriteTrx, hash);
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
void HashIndex<T>::mergeBulkInserts(const Transaction* transaction,
    const InMemHashIndex<T>& insertLocalStorage) {
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
    reserve(transaction, insertLocalStorage.size());
    // RUNTIME_CHECK(auto originalNumEntries = this->indexHeaderForWriteTrx.numEntries);

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
        KUZU_PAGE_SIZE / DiskArray<Slot<T>>::getAlignedElementSize();
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
            sortEntries(transaction, insertLocalStorage, localSlot, partitionedEntries[i]);
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
                        auto merged = mergeSlot(transaction, partitionedEntries[i],
                            diskSlotIterator, diskOverflowSlotIterator, diskSlotId);
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
    // TODO(Guodong): Fix this assertion statement which doesn't count the entries in
    // deleteLocalStorage.
    //     KU_ASSERT(originalNumEntries + insertLocalStorage.getIndexHeader().numEntries ==
    //               indexHeaderForWriteTrx.numEntries);
}

template<typename T>
size_t HashIndex<T>::mergeSlot(const Transaction* transaction,
    const std::vector<HashIndexEntryView>& slotToMerge,
    typename DiskArray<Slot<T>>::WriteIterator& diskSlotIterator,
    typename DiskArray<Slot<T>>::WriteIterator& diskOverflowSlotIterator, slot_id_t diskSlotId) {
    slot_id_t diskEntryPos = 0u;
    // mergeSlot should only be called when there is at least one entry for the given disk slot id
    // in the slot to merge
    Slot<T>* diskSlot = &*diskSlotIterator.seek(diskSlotId);
    KU_ASSERT(diskSlot->header.nextOvfSlotId == SlotHeader::INVALID_OVERFLOW_SLOT_ID ||
              diskOverflowSlotIterator.size() > diskSlot->header.nextOvfSlotId);
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
                    diskOverflowSlotIterator.pushBack(transaction, Slot<T>());
                    KU_ASSERT(
                        diskSlot->header.nextOvfSlotId == SlotHeader::INVALID_OVERFLOW_SLOT_ID ||
                        diskOverflowSlotIterator.size() > diskSlot->header.nextOvfSlotId);
                } else {
                    diskOverflowSlotIterator.seek(diskSlot->header.nextOvfSlotId);
                    KU_ASSERT(
                        diskSlot->header.nextOvfSlotId == SlotHeader::INVALID_OVERFLOW_SLOT_ID ||
                        diskOverflowSlotIterator.size() > diskSlot->header.nextOvfSlotId);
                }
                diskSlot = &*diskOverflowSlotIterator;
                // Check to make sure we're not looping
                KU_ASSERT(diskOverflowSlotIterator.idx() != diskSlot->header.nextOvfSlotId);
                diskEntryPos = 0;
            }
        }
        KU_ASSERT(diskEntryPos < getSlotCapacity<T>());
        diskSlot->entries[diskEntryPos] = *it->entry;
        diskSlot->header.setEntryValid(diskEntryPos, it->fingerprint);
        KU_ASSERT([&]() {
            const auto& key = it->entry->key;
            const auto hash = hashStored(transaction, key);
            const auto primarySlot =
                HashIndexUtils::getPrimarySlotIdForHash(indexHeaderForWriteTrx, hash);
            KU_ASSERT(it->fingerprint == HashIndexUtils::getFingerprintForHash(hash));
            KU_ASSERT(primarySlot == diskSlotId);
            return true;
        }());
        indexHeaderForWriteTrx.numEntries++;
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
    bool inMemMode, PhysicalTypeID keyDataType, BufferManager& bufferManager,
    ShadowFile* shadowFile, VirtualFileSystem* vfs, main::ClientContext* context)
    : keyDataTypeID(keyDataType), fileHandle{bufferManager.getFileHandle(dbFileIDAndName.fName,
                                      inMemMode ? FileHandle::O_PERSISTENT_FILE_IN_MEM :
                                      readOnly  ? FileHandle::O_PERSISTENT_FILE_READ_ONLY :
                                                  FileHandle::O_PERSISTENT_FILE_CREATE_NOT_EXISTS,
                                      vfs, context)},
      dbFileIDAndName{dbFileIDAndName}, shadowFile{*shadowFile} {
    KU_ASSERT(!(inMemMode && readOnly));
    bool newIndex = fileHandle->getNumPages() == 0;

    if (newIndex) {
        fileHandle->addNewPages(INDEX_HEADER_PAGES);
        hashIndexHeadersForReadTrx.resize(NUM_HASH_INDEXES);
        hashIndexHeadersForWriteTrx.resize(NUM_HASH_INDEXES);
    } else {
        size_t headerIdx = 0;
        for (size_t headerPageIdx = 0; headerPageIdx < INDEX_HEADER_PAGES; headerPageIdx++) {
            fileHandle->optimisticReadPage(headerPageIdx, [&](auto* frame) {
                const auto onDiskHeaders = reinterpret_cast<HashIndexHeaderOnDisk*>(frame);
                for (size_t i = 0; i < INDEX_HEADERS_PER_PAGE && headerIdx < NUM_HASH_INDEXES;
                     i++) {
                    hashIndexHeadersForReadTrx.emplace_back(onDiskHeaders[i]);
                    headerIdx++;
                }
            });
        }
        hashIndexHeadersForWriteTrx.assign(hashIndexHeadersForReadTrx.begin(),
            hashIndexHeadersForReadTrx.end());
        KU_ASSERT(headerIdx == NUM_HASH_INDEXES);
    }
    hashIndexDiskArrays =
        std::make_unique<DiskArrayCollection>(*fileHandle, dbFileIDAndName.dbFileID, *shadowFile,
            INDEX_HEADER_PAGES /*firstHeaderPage follows the index header pages*/,
            true /*bypassShadowing*/);

    if (keyDataTypeID == PhysicalTypeID::STRING) {
        if (inMemMode) {
            overflowFile = std::make_unique<InMemOverflowFile>(dbFileIDAndName);
        } else {
            overflowFile = std::make_unique<OverflowFile>(dbFileIDAndName, &bufferManager,
                shadowFile, readOnly, vfs, context);
        }
    }
    if (newIndex) {
        // Each index has a primary slot array and an overflow slot array
        for (size_t i = 0; i < NUM_HASH_INDEXES * 2; i++) {
            hashIndexDiskArrays->addDiskArray();
        }
    }

    hashIndices.reserve(NUM_HASH_INDEXES);
    TypeUtils::visit(
        keyDataTypeID,
        [&](ku_string_t) {
            for (auto i = 0u; i < NUM_HASH_INDEXES; i++) {
                hashIndices.push_back(std::make_unique<HashIndex<ku_string_t>>(dbFileIDAndName,
                    fileHandle, overflowFile->addHandle(), *hashIndexDiskArrays, i, shadowFile,
                    hashIndexHeadersForReadTrx[i], hashIndexHeadersForWriteTrx[i]));
            }
        },
        [&]<HashablePrimitive T>(T) {
            for (auto i = 0u; i < NUM_HASH_INDEXES; i++) {
                hashIndices.push_back(std::make_unique<HashIndex<T>>(dbFileIDAndName, fileHandle,
                    nullptr, *hashIndexDiskArrays, i, shadowFile, hashIndexHeadersForReadTrx[i],
                    hashIndexHeadersForWriteTrx[i]));
            }
        },
        [&](auto) { KU_UNREACHABLE; });
}

bool PrimaryKeyIndex::lookup(const Transaction* trx, ValueVector* keyVector, uint64_t vectorPos,
    offset_t& result, visible_func isVisible) {
    bool retVal = false;
    TypeUtils::visit(
        keyDataTypeID,
        [&]<IndexHashable T>(T) {
            T key = keyVector->getValue<T>(vectorPos);
            retVal = lookup(trx, key, result, isVisible);
        },
        [](auto) { KU_UNREACHABLE; });
    return retVal;
}

bool PrimaryKeyIndex::insert(const Transaction* transaction, ValueVector* keyVector,
    uint64_t vectorPos, offset_t value, visible_func isVisible) {
    bool result = false;
    TypeUtils::visit(
        keyDataTypeID,
        [&]<IndexHashable T>(T) {
            T key = keyVector->getValue<T>(vectorPos);
            result = insert(transaction, key, value, isVisible);
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
    bool indexChanged = false;
    for (auto i = 0u; i < NUM_HASH_INDEXES; i++) {
        if (hashIndices[i]->checkpointInMemory()) {
            indexChanged = true;
        }
    }
    if (indexChanged) {
        for (size_t i = 0; i < NUM_HASH_INDEXES; i++) {
            hashIndexHeadersForReadTrx[i] = hashIndexHeadersForWriteTrx[i];
        }
        hashIndexDiskArrays->checkpointInMemory();
    }
    if (overflowFile) {
        overflowFile->checkpointInMemory();
    }
}

void PrimaryKeyIndex::writeHeaders() {
    size_t headerIdx = 0;
    for (size_t headerPageIdx = 0; headerPageIdx < INDEX_HEADER_PAGES; headerPageIdx++) {
        ShadowUtils::updatePage(*fileHandle, dbFileIDAndName.dbFileID, headerPageIdx,
            true /*writing all the data to the page; no need to read original*/, shadowFile,
            [&](auto* frame) {
                auto onDiskFrame = reinterpret_cast<HashIndexHeaderOnDisk*>(frame);
                for (size_t i = 0; i < INDEX_HEADERS_PER_PAGE && headerIdx < NUM_HASH_INDEXES;
                     i++) {
                    hashIndexHeadersForWriteTrx[headerIdx++].write(onDiskFrame[i]);
                }
            });
    }
    KU_ASSERT(headerIdx == NUM_HASH_INDEXES);
}

void PrimaryKeyIndex::checkpoint() {
    bool indexChanged = false;
    for (auto i = 0u; i < NUM_HASH_INDEXES; i++) {
        if (hashIndices[i]->checkpoint()) {
            indexChanged = true;
        }
    }
    if (indexChanged) {
        writeHeaders();
        hashIndexDiskArrays->checkpoint();
    }
    if (overflowFile) {
        overflowFile->checkpoint();
    }
    // Make sure that changes which bypassed the WAL are written.
    // There is no other mechanism for enforcing that they are flushed
    // and they will be dropped when the file handle is destroyed.
    // TODO: Should eventually be moved into the disk array when the disk array can
    // generally handle bypassing the WAL, but should only be run once per file, not once per
    // disk array
    fileHandle->flushAllDirtyPagesInFrames();
    checkpointInMemory();
}

PrimaryKeyIndex::~PrimaryKeyIndex() = default;

} // namespace storage
} // namespace kuzu
