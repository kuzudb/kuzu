#include "src/storage/include/index/hash_index.h"

#include <cstring>

#include "src/common/include/operations/hash_operations.h"

using namespace std;

namespace graphflow {
namespace storage {

HashIndexHeader::HashIndexHeader(DataType keyDataType) : keyDataType{keyDataType} {
    numBytesPerEntry = TypeUtils::getDataTypeSize(keyDataType) + sizeof(node_offset_t);
    numBytesPerSlot = (numBytesPerEntry * slotCapacity) + sizeof(SlotHeader);
    numSlotsPerPage = PAGE_SIZE / numBytesPerSlot;
}

void HashIndexHeader::incrementLevel() {
    currentLevel++;
    levelHashMask = (1 << currentLevel) - 1;
    higherLevelHashMask = (1 << (currentLevel + 1)) - 1;
}

HashIndex::HashIndex(DataType keyDataType)
    : indexHeader{keyDataType}, logger(LoggerUtils::getOrCreateSpdLogger("storage")) {
    // When the Hash index is initialized without the BufferManager, it is opened for only writing.
    // The default for this configuration, has 2 slots and a single primary page.
    primaryPages.reserve(1);
    primaryPages.emplace_back(getNewPage());
    // We intend ovfPages to start dispensing slots from ovfSlotId 1 because default values of
    // ovfSlotId in slot headers is 0 which we reserve for null.
    ovfPages.reserve(1);
    ovfPages.emplace_back(getNewPage());
}

HashIndex::HashIndex(const string& fName, BufferManager& bufferManager, bool isInMemory)
    : logger(LoggerUtils::getOrCreateSpdLogger("storage")), bufferManager{&bufferManager} {
    fileHandle = make_unique<FileHandle>(fName, isInMemory);
    unique_ptr<uint8_t[]> buffer{getNewPage()};
    fileHandle->readPage(buffer.get(), 0);
    indexHeader = *((HashIndexHeader*)buffer.get());
}

void HashIndex::bulkReserve(uint32_t numEntries) {
    // bulkReserve is only safe to be performed once and at the beginning.
    assert(indexHeader.numEntries == 0);
    assert(indexHeader.numPrimarySlots == 2);
    auto capacity = (uint64_t)(numEntries * DEFAULT_HT_LOAD_FACTOR);
    auto requiredNumSlots = capacity / indexHeader.slotCapacity;
    if (capacity % indexHeader.slotCapacity != 0) {
        requiredNumSlots++;
    }
    if (requiredNumSlots <= 2) {
        return;
    }
    indexHeader.numPrimarySlots = requiredNumSlots;
    auto lvlPower2 = 1 << indexHeader.currentLevel;
    while (lvlPower2 * 2 < requiredNumSlots) {
        indexHeader.incrementLevel();
        lvlPower2 = lvlPower2 << 1;
    }
    indexHeader.nextSplitSlotId = indexHeader.numPrimarySlots - lvlPower2;
    indexHeader.numPrimaryPages = requiredNumSlots / indexHeader.numSlotsPerPage;
    if (requiredNumSlots % indexHeader.numSlotsPerPage != 0) {
        indexHeader.numPrimaryPages++;
    }
    // pre-allocate all pages.
    // first page has already been allocated and page 0 is reserved.
    for (auto i = 1; i < indexHeader.numPrimaryPages; i++) {
        primaryPages.emplace_back(getNewPage());
    }
}

// First compute the hash, then check if key already exist, finally insert non-existing key.
// For now, we don't support re-write of key. Existing key will not be inserted.
// Return: true, insertion succeeds. false, insertion failed due to that the key already exists.
bool HashIndex::insert(uint8_t* key, uint64_t value) {
    auto slot = getSlotFromPrimaryPages(calculateSlotIdForHash(getHashOfKey(key)));
    if (!notExistsInSlot(slot, key)) {
        return false;
    }
    auto slotHeader = reinterpret_cast<SlotHeader*>(slot);
    while (slotHeader->nextOvfSlotId != 0) {
        slot = getOvfSlotFromOvfPages(slotHeader->nextOvfSlotId);
        if (!notExistsInSlot(slot, key)) {
            return false;
        }
        slotHeader = reinterpret_cast<SlotHeader*>(slot);
    }
    if (slotHeader->numEntries == indexHeader.slotCapacity) {
        slotHeader->nextOvfSlotId = reserveOvfSlot();
        slot = getOvfSlotFromOvfPages(slotHeader->nextOvfSlotId);
    }
    putNewEntryInSlotAndUpdateHeader(slot, key, value);
    return true;
}

void HashIndex::putNewEntryInSlotAndUpdateHeader(uint8_t* slot, uint8_t* key, node_offset_t value) {
    auto slotHeader = reinterpret_cast<SlotHeader*>(slot);
    auto entry = getEntryInSlot(slot, slotHeader->numEntries);
    memcpy(entry, key, TypeUtils::getDataTypeSize(indexHeader.keyDataType));
    entry += TypeUtils::getDataTypeSize(indexHeader.keyDataType);
    memcpy(entry, &value, sizeof(node_offset_t));
    indexHeader.numEntries += 1;
    slotHeader->numEntries++;
}

bool HashIndex::notExistsInSlot(uint8_t* slot, uint8_t* key) {
    auto slotHeader = reinterpret_cast<SlotHeader*>(slot);
    for (auto entryPos = 0u; entryPos < slotHeader->numEntries; entryPos++) {
        auto keyInSlot = getEntryInSlot(slot, entryPos);
        if (memcmp(key, keyInSlot, TypeUtils::getDataTypeSize(indexHeader.keyDataType)) == 0) {
            return false;
        }
    }
    return true;
}

void HashIndex::saveToDisk(const string& fName) {
    auto fileInfo = FileUtils::openFile(fName, O_WRONLY | O_CREAT);
    unique_ptr<uint8_t[]> buffer(getNewPage());
    memcpy(buffer.get(), &indexHeader, sizeof(indexHeader));
    auto writeOffset = 0u;
    FileUtils::writeToFile(fileInfo.get(), buffer.get(), PAGE_SIZE, writeOffset);
    writeOffset += PAGE_SIZE;
    for (auto& primaryPage : primaryPages) {
        FileUtils::writeToFile(fileInfo.get(), primaryPage.get(), PAGE_SIZE, writeOffset);
        writeOffset += PAGE_SIZE;
    }
    for (auto& ovfPage : ovfPages) {
        FileUtils::writeToFile(fileInfo.get(), ovfPage.get(), PAGE_SIZE, writeOffset);
        writeOffset += PAGE_SIZE;
    }
    FileUtils::closeFile(fileInfo->fd);
}

bool HashIndex::lookup(uint8_t* key, node_offset_t& result, BufferManagerMetrics& metrics) {
    auto slotId = calculateSlotIdForHash(getHashOfKey(key));
    auto pageId = getPageIdForSlot(slotId);
    auto slotIdInPage = getSlotIdInPageForSlot(slotId);
    auto physicalPageId = pageId + 1;
    auto page = bufferManager->pin(*fileHandle, physicalPageId, metrics);
    auto slot = getSlotInAPage(page, slotIdInPage);
    auto foundKey = lookupInSlot(slot, key, result);
    if (foundKey) {
        bufferManager->unpin(*fileHandle, physicalPageId);
        return true;
    }
    auto slotHeader = reinterpret_cast<SlotHeader*>(const_cast<uint8_t*>(slot));
    while (slotHeader->nextOvfSlotId != 0) {
        pageId = getPageIdForSlot(slotHeader->nextOvfSlotId);
        slotIdInPage = getSlotIdInPageForSlot(slotHeader->nextOvfSlotId);
        bufferManager->unpin(*fileHandle, physicalPageId);
        physicalPageId = 1 + indexHeader.numPrimaryPages + pageId;
        page = bufferManager->pin(*fileHandle, physicalPageId, metrics);
        slot = getSlotInAPage(page, slotIdInPage);
        foundKey = lookupInSlot(slot, key, result);
        if (foundKey) {
            bufferManager->unpin(*fileHandle, physicalPageId);
            return true;
        }
        slotHeader = reinterpret_cast<SlotHeader*>(const_cast<uint8_t*>(slot));
    }
    bufferManager->unpin(*fileHandle, physicalPageId);
    return false;
}

bool HashIndex::lookupInSlot(const uint8_t* slot, uint8_t* key, node_offset_t& result) const {
    auto slotHeader = reinterpret_cast<SlotHeader*>(const_cast<uint8_t*>(slot));
    for (auto entryPos = 0u; entryPos < slotHeader->numEntries; entryPos++) {
        auto keyInSlot = getEntryInSlot(const_cast<uint8_t*>(slot), entryPos);
        if (memcmp(key, keyInSlot, TypeUtils::getDataTypeSize(indexHeader.keyDataType)) == 0) {
            result =
                *(node_offset_t*)(keyInSlot + TypeUtils::getDataTypeSize(indexHeader.keyDataType));
            return true;
        }
    }
    return false;
}

uint64_t HashIndex::calculateSlotIdForHash(hash_t hash) const {
    auto slotId = hash & indexHeader.levelHashMask;
    slotId =
        slotId >= indexHeader.nextSplitSlotId ? slotId : (hash & indexHeader.higherLevelHashMask);
    return slotId;
}

uint8_t* HashIndex::getSlotFromPrimaryPages(uint64_t slotId) const {
    return const_cast<uint8_t*>(getSlotInAPage(
        primaryPages[getPageIdForSlot(slotId)].get(), getSlotIdInPageForSlot(slotId)));
}

uint8_t* HashIndex::getOvfSlotFromOvfPages(uint64_t slotId) const {
    return const_cast<uint8_t*>(
        getSlotInAPage(ovfPages[getPageIdForSlot(slotId)].get(), getSlotIdInPageForSlot(slotId)));
}

uint64_t HashIndex::reserveOvfSlot() {
    if (indexHeader.numOvfSlots % indexHeader.numSlotsPerPage == 0) {
        ovfPages.emplace_back(getNewPage());
        indexHeader.numOvfPages++;
    }
    return indexHeader.numOvfSlots++;
}

uint8_t* HashIndex::getNewPage() {
    auto newPage = new uint8_t[PAGE_SIZE];
    memset(newPage, 0, PAGE_SIZE);
    return newPage;
}

hash_t HashIndex::getHashOfKey(uint8_t* key) {
    hash_t hash;
    if (indexHeader.keyDataType == INT64) {
        operation::Hash::operation(*(int64_t*)(key), false /*isNULL*/, hash);
    } else {
        throw invalid_argument("key other than INT64 not supported");
    }
    return hash;
}

} // namespace storage
} // namespace graphflow
