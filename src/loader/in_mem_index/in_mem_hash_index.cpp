#include "src/loader/in_mem_index/include/in_mem_hash_index.h"

namespace graphflow {
namespace loader {

InMemHashIndexBuilder::InMemHashIndexBuilder(string fName, const DataType& keyDataType)
    : fName{move(fName)} {
    assert(keyDataType.typeID == INT64 || keyDataType.typeID == STRING);
    inMemFile = make_unique<InMemFile>(
        this->fName, 1, false, 8 /* initialize the hash index file with 8 pages. */);
    if (keyDataType.typeID == STRING) {
        inMemOverflowFile =
            make_unique<InMemOverflowFile>(StorageUtils::getOverflowPagesFName(this->fName));
    }
    indexHeader = make_unique<HashIndexHeader>(keyDataType.typeID);
    // Initialize functions.
    keyHashFunc = HashIndexUtils::initializeHashFunc(indexHeader->keyDataTypeID);
    keyInsertFunc = InMemHashIndexUtils::initializeInsertFunc(indexHeader->keyDataTypeID);
    keyEqualsFunc = InMemHashIndexUtils::initializeEqualsFunc(indexHeader->keyDataTypeID);
}

void InMemHashIndexBuilder::bulkReserve(uint32_t numEntries_) {
    // bulkReserve is only safe to be performed only once when the index is empty.
    assert(indexHeader->numEntries == 0);
    auto capacity = (uint64_t)(numEntries_ * DEFAULT_HT_LOAD_FACTOR);
    auto requiredNumSlots = capacity / HashIndexConfig::SLOT_CAPACITY;
    if (capacity % HashIndexConfig::SLOT_CAPACITY != 0) {
        requiredNumSlots++;
    }
    auto lvlPower2 = 1 << indexHeader->currentLevel;
    while ((lvlPower2 << 1) < requiredNumSlots) {
        indexHeader->incrementLevel();
        lvlPower2 = lvlPower2 << 1;
    }
    indexHeader->nextSplitSlotId = requiredNumSlots - lvlPower2;
    auto requiredNumPages = requiredNumSlots / indexHeader->numSlotsPerPage;
    if (requiredNumSlots % indexHeader->numSlotsPerPage != 0) {
        requiredNumPages++;
    }
    // Pre-allocate all pages. The first page has already been allocated and page 0 is reserved.
    for (auto i = 0; i < requiredNumPages; i++) {
        inMemFile->addANewPage(true /* setToZero */);
    }
    auto previousNumSlots = slotMutexes.size();
    numSlots += requiredNumSlots;
    slotsCapacity += requiredNumPages * indexHeader->numSlotsPerPage;
    slotMutexes.resize(slotsCapacity);
    for (auto i = previousNumSlots; i < slotsCapacity; i++) {
        slotMutexes[i] = make_unique<mutex>();
    }
}

bool InMemHashIndexBuilder::existsInSlot(uint8_t* slot, const uint8_t* key) const {
    auto slotHeader = reinterpret_cast<SlotHeader*>(slot);
    for (auto entryPos = 0u; entryPos < slotHeader->numEntries; entryPos++) {
        if (slotHeader->isEntryDeleted(entryPos)) {
            continue;
        }
        auto entry = getEntryInSlot(const_cast<uint8_t*>(slot), entryPos);
        if (keyEqualsFunc(key, entry, inMemOverflowFile.get())) {
            return true;
        }
    }
    return false;
}

void InMemHashIndexBuilder::insertToSlot(uint8_t* slot, const uint8_t* key, node_offset_t value) {
    auto slotHeader = reinterpret_cast<SlotHeader*>(slot);
    auto entry = getEntryInSlot(slot, slotHeader->numEntries);
    keyInsertFunc(key, value, entry, inMemOverflowFile.get());
    slotHeader->numEntries++;
}

uint64_t InMemHashIndexBuilder::reserveSlot() {
    unique_lock lck{lockForReservingOvfSlot};
    if (numSlots >= slotsCapacity) {
        unique_lock u_lck{sharedLockForSlotMutexes};
        inMemFile->addANewPage(true /* setToZero */);
        for (auto i = 0u; i < indexHeader->numSlotsPerPage; i++) {
            slotMutexes.push_back(make_unique<mutex>());
        }
        slotsCapacity += indexHeader->numSlotsPerPage;
        assert(slotsCapacity == slotMutexes.size());
    }
    return numSlots++;
}

bool InMemHashIndexBuilder::appendInternal(const uint8_t* key, node_offset_t value) {
    uint64_t slotId = UINT64_MAX;
    uint64_t nextSlotId = calculateSlotIdForHash(keyHashFunc(key));
    uint8_t* slot = nullptr;
    SlotHeader* slotHeader = nullptr;
    while (slotId == UINT64_MAX || nextSlotId != 0) {
        lockSlot(nextSlotId);
        slot = getSlot(nextSlotId);
        if (slotId != UINT64_MAX) {
            unlockSlot(slotId);
        }
        slotId = nextSlotId;
        if (existsInSlot(slot, key)) {
            unlockSlot(slotId);
            return false;
        }
        slotHeader = reinterpret_cast<SlotHeader*>(slot);
        nextSlotId = slotHeader->nextOvfSlotId;
    }
    assert(slotHeader != nullptr);
    if (slotHeader->numEntries == HashIndexConfig::SLOT_CAPACITY) {
        // Allocate a new ovf slot.
        nextSlotId = reserveSlot();
        lockSlot(nextSlotId);
        slot = getSlot(nextSlotId);
        slotHeader->nextOvfSlotId = nextSlotId;
        unlockSlot(slotId);
        slotId = nextSlotId;
    }
    insertToSlot(slot, key, value);
    unlockSlot(slotId);
    numEntries.fetch_add(1);
    return true;
}

void InMemHashIndexBuilder::flush() {
    // Copy index header back to the page.
    indexHeader->numEntries = numEntries.load();
    // Populate and flush the index header page.
    memcpy(inMemFile->getPage(INDEX_HEADER_PAGE_ID)->data, (uint8_t*)indexHeader.get(),
        sizeof(HashIndexHeader));
    // Flush files.
    inMemFile->flush();
    if (inMemOverflowFile) {
        inMemOverflowFile->flush();
    }
}

} // namespace loader
} // namespace graphflow
