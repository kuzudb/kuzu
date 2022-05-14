#include "src/storage/include/index/hash_index.h"

using namespace std;

namespace graphflow {
namespace storage {

HashIndexHeader::HashIndexHeader(DataTypeID keyDataTypeID) : keyDataTypeID{keyDataTypeID} {
    numBytesPerEntry = Types::getDataTypeSize(this->keyDataTypeID) + sizeof(node_offset_t);
    numBytesPerSlot = (numBytesPerEntry * HashIndexConfig::SLOT_CAPACITY) + sizeof(SlotHeader);
    assert(numBytesPerSlot < DEFAULT_PAGE_SIZE);
    numSlotsPerPage = DEFAULT_PAGE_SIZE / numBytesPerSlot;
}

void HashIndexHeader::incrementLevel() {
    currentLevel++;
    levelHashMask = (1 << currentLevel) - 1;
    higherLevelHashMask = (1 << (currentLevel + 1)) - 1;
}

HashIndex::HashIndex(
    string fName, const DataType& keyDataType, BufferManager& bufferManager, bool isInMemory)
    : fName{move(fName)}, bm{bufferManager} {
    assert(keyDataType.typeID == INT64 || keyDataType.typeID == STRING);
    fh = make_unique<FileHandle>(
        this->fName, FileHandle::O_DefaultPagedExistingDBFileCreateIfNotExists);
    initializeHeaderAndPages(keyDataType, isInMemory);
    // Initialize functions.
    keyHashFunc = HashIndexUtils::initializeHashFunc(indexHeader->keyDataTypeID);
    insertKeyToEntryFunc =
        HashIndexUtils::initializeInsertKeyToEntryFunc(indexHeader->keyDataTypeID);
    equalsFuncInWrite = HashIndexUtils::initializeEqualsFuncInWriteMode(indexHeader->keyDataTypeID);
    equalsFuncInRead = HashIndexUtils::initializeEqualsFuncInReadMode(indexHeader->keyDataTypeID);
}

HashIndex::~HashIndex() {
    bm.flushAllDirtyPagesInFrames(*fh);
}

void HashIndex::initializeHeaderAndPages(const DataType& keyDataType, bool isInMemory) {
    if (fh->getNumPages() == 0) {
        indexHeader = make_unique<HashIndexHeader>(keyDataType.typeID);
        // Allocate the index header page, which is always the first page in the index file.
        auto headerPageIdx = fh->addNewPage();
        assert(headerPageIdx == INDEX_HEADER_PAGE_ID);
        // Allocate the first primary page.
        allocateAndCachePageWithoutLock(true /* isPrimary */);
        // Initialize mutexes for initial primary slots.
        primarySlotMutexes.resize(1 << indexHeader->currentLevel);
        for (auto i = 0u; i < (1 << indexHeader->currentLevel); i++) {
            primarySlotMutexes[i] = make_unique<mutex>();
        }
        inMemStringOvfPages =
            indexHeader->keyDataTypeID == STRING ?
                make_unique<InMemOverflowPages>(OverflowPages::getOverflowPagesFName(fName)) :
                nullptr;
    } else {
        // Read the index header from file.
        auto buffer = bm.pin(*fh, INDEX_HEADER_PAGE_ID);
        indexHeader = make_unique<HashIndexHeader>(*(HashIndexHeader*)buffer);
        bm.unpin(*fh, INDEX_HEADER_PAGE_ID);
        assert(indexHeader->keyDataTypeID == keyDataType.typeID);
        stringOvfPages = indexHeader->keyDataTypeID == STRING ?
                             make_unique<OverflowPages>(fName, bm, isInMemory) :
                             nullptr;
        readLogicalToPhysicalPageMappings();
    }
}

void HashIndex::bulkReserve(uint32_t numEntries_) {
    // bulkReserve is only safe to be performed only once when the index is empty.
    assert(indexHeader->numEntries == 0);
    auto capacity = (uint64_t)(numEntries_ * DEFAULT_HT_LOAD_FACTOR);
    auto requiredNumSlots = capacity / HashIndexConfig::SLOT_CAPACITY;
    if (capacity % HashIndexConfig::SLOT_CAPACITY != 0) {
        requiredNumSlots++;
    }
    if (requiredNumSlots <= 2) {
        return;
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
    for (auto i = 1; i < requiredNumPages; i++) {
        allocateAndCachePageWithoutLock(true /* isPrimary */);
    }
    auto previousNumSlots = primarySlotMutexes.size();
    primarySlotMutexes.resize(requiredNumSlots);
    for (auto i = previousNumSlots; i < requiredNumSlots; i++) {
        primarySlotMutexes[i] = make_unique<mutex>();
    }
}

// First compute the hash, then check if key already exist, finally insert non-existing key.
// For now, we don't support re-write of key. Existing key will not be inserted.
// Return: true, insertion succeeds. false, insertion failed due to the key already existing.
bool HashIndex::insertInternal(const uint8_t* key, uint64_t value) {
    SlotInfo slotInfo{UINT32_MAX, true};
    SlotInfo nextSlotInfo{UINT32_MAX, true};
    uint8_t* slot = nullptr;
    SlotHeader* slotHeader = nullptr;
    nextSlotInfo.slotId = calculateSlotIdForHash(keyHashFunc(key));
    while (nextSlotInfo.isPrimary || nextSlotInfo.slotId != 0) {
        lockSlot(nextSlotInfo);
        if (slotInfo.slotId != UINT32_MAX) {
            unLockSlot(slotInfo);
        }
        slotInfo = nextSlotInfo;
        slot = getSlotFromPages(slotInfo);
        if (existsInSlot(slot, key)) {
            unLockSlot(slotInfo);
            return false;
        }
        slotHeader = reinterpret_cast<SlotHeader*>(slot);
        nextSlotInfo.slotId = slotHeader->nextOvfSlotId;
        nextSlotInfo.isPrimary = false;
    }
    if (slotHeader->numEntries == HashIndexConfig::SLOT_CAPACITY) {
        // Allocate a new ovf slot.
        nextSlotInfo.slotId = reserveOvfSlot();
        nextSlotInfo.isPrimary = false;
        lockSlot(nextSlotInfo);
        slot = getSlotFromPages(nextSlotInfo);
        slotHeader->nextOvfSlotId = nextSlotInfo.slotId;
        unLockSlot(slotInfo);
        slotInfo = nextSlotInfo;
    }
    insertToSlot(slot, key, value);
    unLockSlot(slotInfo);
    numEntries.fetch_add(1);
    return true;
}

void HashIndex::insertToSlot(uint8_t* slot, const uint8_t* key, node_offset_t value) {
    auto slotHeader = reinterpret_cast<SlotHeader*>(slot);
    auto entry = getEntryInSlot(slot, slotHeader->numEntries);
    insertKeyToEntryFunc(key, entry, inMemStringOvfPages.get(), &stringOvfPageCursor);
    entry += Types::getDataTypeSize(indexHeader->keyDataTypeID);
    memcpy(entry, &value, sizeof(node_offset_t));
    slotHeader->numEntries++;
}

bool HashIndex::existsInSlot(uint8_t* slot, const uint8_t* key) {
    auto slotHeader = reinterpret_cast<SlotHeader*>(slot);
    for (auto entryPos = 0u; entryPos < slotHeader->numEntries; entryPos++) {
        auto keyInEntry = getEntryInSlot(slot, entryPos);
        bool foundKey = equalsFuncInWrite(key, keyInEntry, inMemStringOvfPages.get());
        if (foundKey) {
            return true;
        }
    }
    return false;
}

bool HashIndex::lookupInternal(const uint8_t* key, node_offset_t& result) {
    auto slotId = calculateSlotIdForHash(keyHashFunc(key));
    auto pageId = getPageIdForSlot(slotId);
    auto slotIdInPage = getSlotIdInPageForSlot(slotId);
    auto physicalPageId = primaryLogicalToPhysicalPagesMapping[pageId];
    auto page = bm.pin(*fh, physicalPageId);
    auto slot = getSlotInPage(page, slotIdInPage);
    auto foundKey = lookupInSlot(slot, key, result);
    if (foundKey) {
        bm.unpin(*fh, physicalPageId);
        return true;
    }
    auto slotHeader = reinterpret_cast<SlotHeader*>(const_cast<uint8_t*>(slot));
    while (slotHeader->nextOvfSlotId != 0) {
        pageId = getPageIdForSlot(slotHeader->nextOvfSlotId);
        slotIdInPage = getSlotIdInPageForSlot(slotHeader->nextOvfSlotId);
        bm.unpin(*fh, physicalPageId);
        physicalPageId = ovfLogicalToPhysicalPagesMapping[pageId];
        page = bm.pin(*fh, physicalPageId);
        slot = getSlotInPage(page, slotIdInPage);
        foundKey = lookupInSlot(slot, key, result);
        if (foundKey) {
            bm.unpin(*fh, physicalPageId);
            return true;
        }
        slotHeader = reinterpret_cast<SlotHeader*>(const_cast<uint8_t*>(slot));
    }
    bm.unpin(*fh, physicalPageId);
    return false;
}

bool HashIndex::lookupInSlot(const uint8_t* slot, const uint8_t* key, node_offset_t& result) const {
    auto slotHeader = reinterpret_cast<SlotHeader*>(const_cast<uint8_t*>(slot));
    for (auto entryPos = 0u; entryPos < slotHeader->numEntries; entryPos++) {
        auto keyInEntry = getEntryInSlot(const_cast<uint8_t*>(slot), entryPos);
        bool foundKey = equalsFuncInRead(key, keyInEntry, stringOvfPages.get());
        if (foundKey) {
            result =
                *(node_offset_t*)(keyInEntry + Types::getDataTypeSize(indexHeader->keyDataTypeID));
            return true;
        }
    }
    return false;
}

uint64_t HashIndex::calculateSlotIdForHash(hash_t hash) {
    auto slotId = hash & indexHeader->levelHashMask;
    slotId =
        slotId >= indexHeader->nextSplitSlotId ? slotId : (hash & indexHeader->higherLevelHashMask);
    return slotId;
}

uint8_t* HashIndex::getSlotFromPages(const SlotInfo& slotInfo) {
    if (slotInfo.isPrimary) {
        auto logicalPageIdx = getPageIdForSlot(slotInfo.slotId);
        return const_cast<uint8_t*>(getSlotInPage(
            primaryPinnedFrames[logicalPageIdx], getSlotIdInPageForSlot(slotInfo.slotId)));
    } else {
        shared_lock s_lck{sharedLockForSlotMutexes};
        auto logicalPageIdx = getPageIdForSlot(slotInfo.slotId);
        return const_cast<uint8_t*>(getSlotInPage(
            ovfPinnedFrames[logicalPageIdx], getSlotIdInPageForSlot(slotInfo.slotId)));
    }
}

uint64_t HashIndex::reserveOvfSlot() {
    unique_lock lck{lockForReservingOvfSlot};
    if (numOvfSlots >= ovfSlotsCapacity) {
        unique_lock u_lck{sharedLockForSlotMutexes};
        allocateAndCachePageWithoutLock(false /* isPrimary */);
        for (auto i = 0u; i < indexHeader->numSlotsPerPage; i++) {
            ovfSlotMutexes.push_back(make_unique<mutex>());
        }
        ovfSlotsCapacity += indexHeader->numSlotsPerPage;
        assert(ovfSlotsCapacity == ovfSlotMutexes.size());
    }
    return numOvfSlots++;
}

void HashIndex::allocateAndCachePageWithoutLock(bool isPrimary) {
    auto physicalPageIdx = fh->addNewPage();
    auto data = bm.pinWithoutReadingFromFile(*fh, physicalPageIdx);
    memset(data, 0, DEFAULT_PAGE_SIZE);
    if (isPrimary) {
        primaryPinnedFrames.push_back(data);
        primaryLogicalToPhysicalPagesMapping.push_back(physicalPageIdx);
    } else {
        ovfPinnedFrames.push_back(data);
        ovfLogicalToPhysicalPagesMapping.push_back(physicalPageIdx);
    }
}

void HashIndex::lockSlot(const SlotInfo& slotInfo) {
    assert(slotInfo.slotId != UINT32_MAX);
    if (slotInfo.isPrimary) {
        primarySlotMutexes[slotInfo.slotId]->lock();
    } else {
        shared_lock lck{sharedLockForSlotMutexes};
        ovfSlotMutexes[slotInfo.slotId]->lock();
    }
}

void HashIndex::unLockSlot(const SlotInfo& slotInfo) {
    if (slotInfo.isPrimary) {
        primarySlotMutexes[slotInfo.slotId]->unlock();
    } else {
        shared_lock lck{sharedLockForSlotMutexes};
        ovfSlotMutexes[slotInfo.slotId]->unlock();
    }
}

void HashIndex::readLogicalToPhysicalPageMappings() {
    // Skip the index header, primary, and ovf pages.
    auto readOffset =
        (indexHeader->numPrimaryPages + indexHeader->numOvfPages + 1) * DEFAULT_PAGE_SIZE;
    primaryLogicalToPhysicalPagesMapping.resize(indexHeader->numPrimaryPages);
    FileUtils::readFromFile(fh->getFileInfo(), &primaryLogicalToPhysicalPagesMapping[0],
        indexHeader->numPrimaryPages * sizeof(uint32_t), readOffset);
    readOffset += indexHeader->numPrimaryPages * sizeof(uint32_t);
    ovfLogicalToPhysicalPagesMapping.resize(indexHeader->numOvfPages);
    FileUtils::readFromFile(fh->getFileInfo(), &ovfLogicalToPhysicalPagesMapping[0],
        indexHeader->numOvfPages * sizeof(uint32_t), readOffset);
}

void HashIndex::writeLogicalToPhysicalPageMappings() {
    // Append mappings of both primary and ovf pages to the end of the file.
    auto writeOffset = fh->getNumPages() * DEFAULT_PAGE_SIZE;
    auto numBytesForPrimaryPages = primaryLogicalToPhysicalPagesMapping.size() * sizeof(uint32_t);
    FileUtils::writeToFile(fh->getFileInfo(), (uint8_t*)&primaryLogicalToPhysicalPagesMapping[0],
        numBytesForPrimaryPages, writeOffset);
    writeOffset += numBytesForPrimaryPages;
    FileUtils::writeToFile(fh->getFileInfo(), (uint8_t*)&ovfLogicalToPhysicalPagesMapping[0],
        ovfLogicalToPhysicalPagesMapping.size() * sizeof(uint32_t), writeOffset);
}

void HashIndex::flush() {
    if (primaryLogicalToPhysicalPagesMapping.empty()) {
        return;
    }
    // Copy index header back to the page.
    indexHeader->numEntries = numEntries.load();
    indexHeader->numPrimaryPages = primaryLogicalToPhysicalPagesMapping.size();
    indexHeader->numOvfPages = ovfLogicalToPhysicalPagesMapping.size();
    // Populate and flush the index header page.
    auto headerFrame = bm.pin(*fh, INDEX_HEADER_PAGE_ID);
    memcpy(headerFrame, (uint8_t*)indexHeader.get(), sizeof(HashIndexHeader));
    setDirtyAndUnPinPage(INDEX_HEADER_PAGE_ID);
    // Flush primary and ovf pages.
    for (auto page : primaryLogicalToPhysicalPagesMapping) {
        setDirtyAndUnPinPage(page);
    }
    for (auto page : ovfLogicalToPhysicalPagesMapping) {
        setDirtyAndUnPinPage(page);
    }
    if (indexHeader->keyDataTypeID == STRING) {
        inMemStringOvfPages->saveToFile();
    }
    writeLogicalToPhysicalPageMappings();
}

} // namespace storage
} // namespace graphflow
