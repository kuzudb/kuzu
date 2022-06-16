#include "src/storage/index/include/hash_index.h"

namespace graphflow {
namespace storage {

HashIndex::HashIndex(
    string fName, const DataType& keyDataType, BufferManager& bufferManager, bool isInMemory)
    : fName{move(fName)}, isInMemory{isInMemory}, bm{bufferManager} {
    assert(keyDataType.typeID == INT64 || keyDataType.typeID == STRING);
    initializeFileAndHeader(keyDataType);
    // Initialize functions.
    keyHashFunc = HashIndexUtils::initializeHashFunc(indexHeader->keyDataTypeID);
    keyEqualsFunc = HashIndexUtils::initializeEqualsFunc(indexHeader->keyDataTypeID);
}

HashIndex::~HashIndex() {
    if (isInMemory) {
        StorageStructureUtils::unpinEachPageOfFile(*fh, bm);
    }
}

void HashIndex::initializeFileAndHeader(const DataType& keyDataType) {
    fh = make_unique<FileHandle>(fName, FileHandle::O_DefaultPagedExistingDBFileCreateIfNotExists);
    assert(fh->getNumPages() > 0);
    // Read the index header and page mappings from file.
    auto buffer = bm.pin(*fh, INDEX_HEADER_PAGE_ID);
    auto otherIndexHeader = *(HashIndexHeader*)buffer;
    indexHeader = make_unique<HashIndexHeader>(otherIndexHeader);
    bm.unpin(*fh, INDEX_HEADER_PAGE_ID);
    numEntries.store(indexHeader->numEntries);
    assert(indexHeader->keyDataTypeID == keyDataType.typeID);
    auto slotsCapacity = fh->getNumPages() * indexHeader->numSlotsPerPage;
    slotMutexes.resize(slotsCapacity);
    for (auto i = 0u; i < slotsCapacity; i++) {
        slotMutexes[i] = make_unique<mutex>();
    }
    if (isInMemory) {
        StorageStructureUtils::pinEachPageOfFile(*fh, bm);
    }
    if (indexHeader->keyDataTypeID == STRING) {
        overflowFile = make_unique<OverflowFile>(fName, bm, isInMemory);
    }
}

template<bool IS_DELETE>
bool HashIndex::lookupOrDeleteInSlot(
    uint8_t* slot, const uint8_t* key, node_offset_t* result) const {
    auto slotHeader = reinterpret_cast<SlotHeader*>(slot);
    for (auto entryPos = 0u; entryPos < slotHeader->numEntries; entryPos++) {
        if (slotHeader->isEntryDeleted(entryPos)) {
            continue;
        }
        auto entry = getEntryInSlot(const_cast<uint8_t*>(slot), entryPos);
        if (keyEqualsFunc(key, entry, overflowFile.get())) {
            if constexpr (IS_DELETE) {
                slotHeader->setEntryDeleted(entryPos);
            } else {
                memcpy(result, entry + Types::getDataTypeSize(indexHeader->keyDataTypeID),
                    sizeof(node_offset_t));
            }
            return true;
        }
    }
    return false;
}

// Lookups and Deletions follow the same pattern that they compute the initial slot, and traverse
// the chain to find the matched slot. Differences are that: 1) they perform different actions.
// Lookup copies the matched value, while deletion sets the mask for deleted entry in SlotHeader; 2)
// we assume lookups are not mixed with deletions/insertions, thus they can be performed without
// locks, while deletions require locks in the context of multi-threads.
template<bool IS_DELETE>
bool HashIndex::lookupOrDeleteInternal(const uint8_t* key, node_offset_t* result) {
    auto slotId = UINT64_MAX;
    auto nextSlotId = calculateSlotIdForHash(keyHashFunc(key));
    while (slotId == UINT64_MAX || nextSlotId != 0) {
        slotId = nextSlotId;
        if constexpr (IS_DELETE) {
            lockSlot(slotId);
        }
        auto slot = pinSlot(slotId);
        if (lookupOrDeleteInSlot<IS_DELETE>(slot, key, result)) {
            unpinSlot(slotId);
            if constexpr (IS_DELETE) {
                unlockSlot(slotId);
            }
            return true;
        }
        auto slotHeader = reinterpret_cast<SlotHeader*>(const_cast<uint8_t*>(slot));
        nextSlotId = slotHeader->nextOvfSlotId;
        unpinSlot(slotId);
        if constexpr (IS_DELETE) {
            unlockSlot(slotId);
        }
    }
    return false;
}

// Forward declarations for template functions in the cpp file.
template bool HashIndex::lookupOrDeleteInSlot<true>(
    uint8_t* slot, const uint8_t* key, node_offset_t* result) const;
template bool HashIndex::lookupOrDeleteInSlot<false>(
    uint8_t* slot, const uint8_t* key, node_offset_t* result) const;
template bool HashIndex::lookupOrDeleteInternal<true>(const uint8_t* key, node_offset_t* result);
template bool HashIndex::lookupOrDeleteInternal<false>(const uint8_t* key, node_offset_t* result);

uint8_t* HashIndex::pinSlot(slot_id_t slotId) {
    auto pageIdx = getPhysicalPageIdForSlot(slotId);
    auto frame = bm.pin(*fh, pageIdx);
    auto slotIdxInPage = getSlotIdInPageForSlot(slotId);
    return frame + (slotIdxInPage * indexHeader->numBytesPerSlot);
}

void HashIndex::unpinSlot(slot_id_t slotId) {
    auto pageIdx = getPhysicalPageIdForSlot(slotId);
    bm.unpin(*fh, pageIdx);
}

void HashIndex::flush() {
    // Copy index header back to the page.
    indexHeader->numEntries = numEntries.load();
    // Populate and flush the index header page.
    auto headerFrame = bm.pin(*fh, INDEX_HEADER_PAGE_ID);
    memcpy(headerFrame, (uint8_t*)indexHeader.get(), sizeof(HashIndexHeader));
    fh->writePage(headerFrame, INDEX_HEADER_PAGE_ID);
    bm.unpin(*fh, INDEX_HEADER_PAGE_ID);
    for (auto i = INDEX_HEADER_PAGE_ID; i < fh->getNumPages(); i++) {
        auto frame = bm.pin(*fh, i);
        fh->writePage(frame, i);
        bm.unpin(*fh, i);
    }
}

} // namespace storage
} // namespace graphflow
