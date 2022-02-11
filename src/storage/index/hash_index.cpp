#include "src/storage/include/index/hash_index.h"

#include <cstring>

#include "src/common/include/operations/hash_operations.h"

using namespace std;

namespace graphflow {
namespace storage {

HashIndexHeader::HashIndexHeader(DataType keyDataType) : keyDataType{keyDataType} {
    numBytesPerEntry = TypeUtils::getDataTypeSize(keyDataType) + sizeof(node_offset_t);
    numBytesPerSlot = (numBytesPerEntry * slotCapacity) + sizeof(SlotHeader);
    numSlotsPerPage = DEFAULT_PAGE_SIZE / numBytesPerSlot;
}

void HashIndexHeader::incrementLevel() {
    currentLevel++;
    levelHashMask = (1 << currentLevel) - 1;
    higherLevelHashMask = (1 << (currentLevel + 1)) - 1;
}

// Opens the hash index in the write-mode that can only do insertions. Lookups in this mode are
// undefined.
HashIndex::HashIndex(const string& fName, DataType keyDataType)
    : fName{fName}, indexHeader{keyDataType}, logger(LoggerUtils::getOrCreateSpdLogger("storage")) {
    assert(keyDataType == INT64 || keyDataType == STRING);
    // When the Hash index is initialized without the BufferManager, it is opened for only writing.
    // The default for this configuration, has 2 slots and a single primary page.
    primaryPages.reserve(1);
    primaryPages.emplace_back(getNewPage());
    // We intend ovfPages to start dispensing slots from ovfSlotId 1 because default values of
    // ovfSlotId in slot headers is 0 which we reserve for null.
    ovfPages.reserve(1);
    ovfPages.emplace_back(getNewPage());
    if (keyDataType == STRING) {
        inMemStringOvfPages = make_unique<InMemStringOverflowPages>(
            StringOverflowPages::getStringOverflowPagesFName(fName));
    }
}

// Opens the hash index in the read-mode that can only be used for the lookups. Insertions in this
// mode are undefined.
HashIndex::HashIndex(const string& fName, BufferManager& bufferManager, bool isInMemory)
    : fName{fName},
      logger(LoggerUtils::getOrCreateSpdLogger("storage")), bufferManager{&bufferManager} {
    fileHandle = make_unique<FileHandle>(fName,
        isInMemory ? FileHandle::O_InMemoryDefaultPaged : FileHandle::O_DiskBasedDefaultPage);
    unique_ptr<uint8_t[]> buffer{getNewPage()};
    fileHandle->readPage(buffer.get(), 0);
    indexHeader = *((HashIndexHeader*)buffer.get());
    if (indexHeader.keyDataType == STRING) {
        stringOvfPages = make_unique<StringOverflowPages>(fName, bufferManager, isInMemory);
    }
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
    auto slot = getSlotFromPrimaryPages(calculateSlotIdForHash(hashFunc(key)));
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
    insertKey(key, entry);
    entry += TypeUtils::getDataTypeSize(indexHeader.keyDataType);
    memcpy(entry, &value, sizeof(node_offset_t));
    indexHeader.numEntries += 1;
    slotHeader->numEntries++;
}

void HashIndex::insertKey(uint8_t* key, uint8_t* entry) {
    switch (indexHeader.keyDataType) {
    case INT64:
        memcpy(entry, key, TypeUtils::getDataTypeSize(indexHeader.keyDataType));
        break;
    case STRING: {
        gf_string_t gfString;
        inMemStringOvfPages->setStrInOvfPageAndPtrInEncString(
            reinterpret_cast<const char*>(key), stringOvfPageCursor, &gfString);
        memcpy(entry, &gfString, TypeUtils::getDataTypeSize(indexHeader.keyDataType));
        break;
    }
    default:
        throw invalid_argument(
            "Hash index insertion not defined for dataType other than INT64 and STRING");
    }
}

bool HashIndex::notExistsInSlot(uint8_t* slot, uint8_t* key) {
    auto slotHeader = reinterpret_cast<SlotHeader*>(slot);
    for (auto entryPos = 0u; entryPos < slotHeader->numEntries; entryPos++) {
        auto keyInEntry = getEntryInSlot(slot, entryPos);
        bool foundKey = false;
        switch (indexHeader.keyDataType) {
        case INT64:
            foundKey = equalsInt64(key, keyInEntry);
            break;
        case STRING: {
            auto keyInEntryString = (gf_string_t*)keyInEntry;
            if (likelyEqualsString(key, keyInEntryString)) {
                if (keyInEntryString->len <= gf_string_t::SHORT_STR_LENGTH) {
                    foundKey = memcmp(key, keyInEntryString->prefix, keyInEntryString->len) == 0;
                } else {
                    PageByteCursor cursor;
                    keyInEntryString->getOverflowPtrInfo(cursor.idx, cursor.offset);
                    foundKey = memcmp(key, inMemStringOvfPages->getPtrToMemLoc(cursor),
                                   keyInEntryString->len) == 0;
                }
            }
            break;
        }
        default:
            throw invalid_argument(
                "Hash index equals is not supported for dataType other than INT64 and STRING.");
        }
        if (foundKey) {
            return false;
        }
    }
    return true;
}

void HashIndex::saveToDisk() {
    auto fileInfo = FileUtils::openFile(fName, O_WRONLY | O_CREAT);
    unique_ptr<uint8_t[]> buffer(getNewPage());
    memcpy(buffer.get(), &indexHeader, sizeof(indexHeader));
    auto writeOffset = 0u;
    FileUtils::writeToFile(fileInfo.get(), buffer.get(), DEFAULT_PAGE_SIZE, writeOffset);
    writeOffset += DEFAULT_PAGE_SIZE;
    for (auto& primaryPage : primaryPages) {
        FileUtils::writeToFile(fileInfo.get(), primaryPage.get(), DEFAULT_PAGE_SIZE, writeOffset);
        writeOffset += DEFAULT_PAGE_SIZE;
    }
    for (auto& ovfPage : ovfPages) {
        FileUtils::writeToFile(fileInfo.get(), ovfPage.get(), DEFAULT_PAGE_SIZE, writeOffset);
        writeOffset += DEFAULT_PAGE_SIZE;
    }
    FileUtils::closeFile(fileInfo->fd);
    if (indexHeader.keyDataType == STRING) {
        inMemStringOvfPages->saveToFile();
    }
}

bool HashIndex::lookup(uint8_t* key, node_offset_t& result) {
    auto slotId = calculateSlotIdForHash(hashFunc(key));
    auto pageId = getPageIdForSlot(slotId);
    auto slotIdInPage = getSlotIdInPageForSlot(slotId);
    auto physicalPageId = pageId + 1;
    auto page = bufferManager->pin(*fileHandle, physicalPageId);
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
        page = bufferManager->pin(*fileHandle, physicalPageId);
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
        auto keyInEntry = getEntryInSlot(const_cast<uint8_t*>(slot), entryPos);
        bool foundKey = false;
        switch (indexHeader.keyDataType) {
        case INT64:
            foundKey = equalsInt64(key, keyInEntry);
            break;
        case STRING: {
            auto keyInEntryString = (gf_string_t*)keyInEntry;
            if (likelyEqualsString(key, keyInEntryString)) {
                auto entryKeyString = stringOvfPages->readString(*keyInEntryString);
                foundKey = entryKeyString == string(reinterpret_cast<const char*>(key));
            }
            break;
        }
        default:
            throw invalid_argument(
                "Hash index equals is not supported for dataType other than INT64 and STRING.");
        }
        if (foundKey) {
            result =
                *(node_offset_t*)(keyInEntry + TypeUtils::getDataTypeSize(indexHeader.keyDataType));
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
    auto newPage = new uint8_t[DEFAULT_PAGE_SIZE];
    memset(newPage, 0, DEFAULT_PAGE_SIZE);
    return newPage;
}

hash_t HashIndex::hashFunc(uint8_t* key) {
    hash_t hash;
    switch (indexHeader.keyDataType) {
    case INT64:
        operation::Hash::operation(*(int64_t*)(key), false /*isNULL*/, hash);
        break;
    case STRING:
        hash = std::hash<string>{}(reinterpret_cast<const char*>(key));
        break;
    default:
        throw invalid_argument(
            "Type " + TypeUtils::dataTypeToString(indexHeader.keyDataType) + " not supported.");
    }
    return hash;
}

bool HashIndex::equalsInt64(uint8_t* key, uint8_t* entryKey) {
    return memcmp(key, entryKey, sizeof(int64_t)) == 0;
}

bool HashIndex::likelyEqualsString(uint8_t* key, gf_string_t* entryKey) {
    if (memcmp(key, entryKey->prefix, gf_string_t::PREFIX_LENGTH) != 0) {
        return false;
    }
    if (strlen(reinterpret_cast<const char*>(key)) != entryKey->len) {
        return false;
    }
    return true;
}

} // namespace storage
} // namespace graphflow
