#include "src/storage/include/index/hash_index.h"

using namespace std;

namespace graphflow {
namespace storage {

HashIndexHeader::HashIndexHeader(DataType keyDataType) : keyDataType{move(keyDataType)} {
    numBytesPerEntry = Types::getDataTypeSize(this->keyDataType) + sizeof(node_offset_t);
    numBytesPerSlot = (numBytesPerEntry * slotCapacity) + sizeof(SlotHeader);
    numSlotsPerPage = LARGE_PAGE_SIZE / numBytesPerSlot;
}

void HashIndexHeader::incrementLevel() {
    currentLevel++;
    levelHashMask = (1 << currentLevel) - 1;
    higherLevelHashMask = (1 << (currentLevel + 1)) - 1;
}

hash_function_t HashIndex::initializeHashFunc(const DataType& dataType) {
    switch (dataType.typeID) {
    case INT64:
        return hashFuncForInt64;
    case STRING:
        return hashFuncForString;
    default:
        throw StorageException("Type " + Types::dataTypeToString(dataType) + " not supported.");
    }
}

insert_function_t HashIndex::initializeInsertKeyToEntryFunc(const DataType& dataType) {
    switch (dataType.typeID) {
    case INT64:
        return insertInt64KeyToEntryFunc;
    case STRING:
        return insertStringKeyToEntryFunc;
    default:
        throw StorageException(
            "Hash index insertion not defined for dataType other than INT64 and STRING.");
    }
}

bool HashIndex::isStringPrefixAndLenEquals(
    const uint8_t* keyToLookup, const gf_string_t* keyInEntry) {
    if (memcmp(keyToLookup, keyInEntry->prefix, gf_string_t::PREFIX_LENGTH) != 0) {
        return false;
    }
    if (strlen(reinterpret_cast<const char*>(keyToLookup)) != keyInEntry->len) {
        return false;
    }
    return true;
}

bool HashIndex::equalsFuncInWriteModeForString(const uint8_t* keyToLookup,
    const uint8_t* keyInEntry, const InMemOverflowPages* overflowPages) {
    auto gfStringInEntry = (gf_string_t*)keyInEntry;
    // Checks if prefix and len matches first.
    if (!isStringPrefixAndLenEquals(keyToLookup, gfStringInEntry)) {
        return false;
    }
    if (gfStringInEntry->len <= gf_string_t::PREFIX_LENGTH) {
        // For strings shorter than PREFIX_LENGTH, the result must be true.
        return true;
    } else if (gfStringInEntry->len <= gf_string_t::SHORT_STR_LENGTH) {
        // For short strings, whose lengths are larger than PREFIX_LENGTH, check if their actual
        // values are equal.
        return memcmp(keyToLookup, gfStringInEntry->prefix, gfStringInEntry->len) == 0;
    } else {
        // For long strings, read overflow values and check if they are true.
        PageByteCursor cursor;
        TypeUtils::decodeOverflowPtr(gfStringInEntry->overflowPtr, cursor.idx, cursor.offset);
        return memcmp(keyToLookup, overflowPages->pages[cursor.idx]->data + cursor.offset,
                   gfStringInEntry->len) == 0;
    }
}

equals_in_write_function_t HashIndex::initializeEqualsFuncInWriteMode(const DataType& dataType) {
    switch (dataType.typeID) {
    case INT64:
        return equalsFuncInWriteModeForInt64;
    case STRING:
        return equalsFuncInWriteModeForString;
    default:
        throw StorageException(
            "Hash index equals is not supported for dataType other than INT64 and STRING.");
    }
}

bool HashIndex::equalsFuncInReadModeForString(
    const uint8_t* keyToLookup, const uint8_t* keyInEntry, OverflowPages* overflowPages) {
    auto keyInEntryString = (gf_string_t*)keyInEntry;
    if (isStringPrefixAndLenEquals(keyToLookup, keyInEntryString)) {
        auto entryKeyString = overflowPages->readString(*keyInEntryString);
        return memcmp(keyToLookup, entryKeyString.c_str(), entryKeyString.length()) == 0;
    }
    return false;
}

equals_in_read_function_t HashIndex::initializeEqualsFuncInReadMode(const DataType& dataType) {
    switch (dataType.typeID) {
    case INT64:
        return equalsFuncInReadModeForInt64;
    case STRING:
        return equalsFuncInReadModeForString;
    default:
        throw StorageException(
            "Hash index equals is not supported for dataType other than INT64 and STRING.");
    }
}

// Opens the hash index in the write-mode that can only do insertions. Lookups in this mode are
// undefined.
HashIndex::HashIndex(const string& fName, DataType keyDataType, MemoryManager* memoryManager)
    : fName{fName}, indexMode{WRITE_ONLY}, indexHeader{move(keyDataType)}, memoryManager{
                                                                               memoryManager} {
    assert(memoryManager != nullptr &&
           (indexHeader.keyDataType.typeID == INT64 || indexHeader.keyDataType.typeID == STRING));
    keyHashFunc = initializeHashFunc(indexHeader.keyDataType);
    insertKeyToEntryFunc = initializeInsertKeyToEntryFunc(indexHeader.keyDataType);
    equalsFuncInWrite = initializeEqualsFuncInWriteMode(indexHeader.keyDataType);
    // When the Hash index is initialized without the BufferManager, it is opened for only writing.
    // The default for this configuration, has 2 slots and a single primary page.
    primaryPages.reserve(1);
    primaryPages.emplace_back(memoryManager->allocateBlock(true));
    // We intend ovfPages to start dispensing slots from ovfSlotId 1 because default values of
    // ovfSlotId in slot headers is 0 which we reserve for null.
    ovfPages.reserve(1);
    ovfPages.emplace_back(memoryManager->allocateBlock(true));
    if (indexHeader.keyDataType.typeID == STRING) {
        inMemStringOvfPages =
            make_unique<InMemOverflowPages>(OverflowPages::getOverflowPagesFName(fName));
    }
}

// Opens the hash index in the read-mode that can only be used for the lookups. Insertions in this
// mode are undefined.
HashIndex::HashIndex(const string& fName, BufferManager* bufferManager, bool isInMemory)
    : fName{fName}, indexMode{READ_ONLY}, bufferManager{bufferManager} {
    assert(bufferManager != nullptr);
    fileHandle = make_unique<FileHandle>(fName, FileHandle::O_LargePageExistingDBFileDoNotCreate);
    auto buffer = bufferManager->pin(*fileHandle, 0);
    indexHeader = *((HashIndexHeader*)buffer);
    bufferManager->unpin(*fileHandle, 0);
    keyHashFunc = initializeHashFunc(indexHeader.keyDataType);
    equalsFuncInRead = initializeEqualsFuncInReadMode(indexHeader.keyDataType);
    if (indexHeader.keyDataType.typeID == STRING) {
        stringOvfPages = make_unique<OverflowPages>(fName, *bufferManager, isInMemory);
    }
}

HashIndex::~HashIndex() {
    if (indexMode == WRITE_ONLY) {
        memoryManager->freeBlock(0);
        for (auto& page : primaryPages) {
            memoryManager->freeBlock(page->pageIdx);
        }
        for (auto& page : ovfPages) {
            memoryManager->freeBlock(page->pageIdx);
        }
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
        primaryPages.emplace_back(memoryManager->allocateBlock(true));
    }
}

// First compute the hash, then check if key already exist, finally insert non-existing key.
// For now, we don't support re-write of key. Existing key will not be inserted.
// Return: true, insertion succeeds. false, insertion failed due to that the key already exists.
bool HashIndex::insert(uint8_t* key, uint64_t value) {
    auto slot = getSlotFromPrimaryPages(calculateSlotIdForHash(keyHashFunc(key)));
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
    insertKeyToEntryFunc(key, entry, inMemStringOvfPages.get(), &stringOvfPageCursor);
    entry += Types::getDataTypeSize(indexHeader.keyDataType);
    memcpy(entry, &value, sizeof(node_offset_t));
    indexHeader.numEntries += 1;
    slotHeader->numEntries++;
}

bool HashIndex::notExistsInSlot(uint8_t* slot, uint8_t* key) {
    auto slotHeader = reinterpret_cast<SlotHeader*>(slot);
    for (auto entryPos = 0u; entryPos < slotHeader->numEntries; entryPos++) {
        auto keyInEntry = getEntryInSlot(slot, entryPos);
        bool foundKey = equalsFuncInWrite(key, keyInEntry, inMemStringOvfPages.get());
        if (foundKey) {
            return false;
        }
    }
    return true;
}

void HashIndex::saveToDisk() {
    auto fileInfo = FileUtils::openFile(fName, O_WRONLY | O_CREAT);
    auto writeOffset = 0u;
    FileUtils::writeToFile(
        fileInfo.get(), (uint8_t*)&indexHeader, sizeof(indexHeader), writeOffset);
    writeOffset += LARGE_PAGE_SIZE;
    for (auto& primaryPage : primaryPages) {
        FileUtils::writeToFile(fileInfo.get(), primaryPage->data, LARGE_PAGE_SIZE, writeOffset);
        writeOffset += LARGE_PAGE_SIZE;
    }
    for (auto& ovfPage : ovfPages) {
        FileUtils::writeToFile(fileInfo.get(), ovfPage->data, LARGE_PAGE_SIZE, writeOffset);
        writeOffset += LARGE_PAGE_SIZE;
    }
    FileUtils::closeFile(fileInfo->fd);
    if (indexHeader.keyDataType.typeID == STRING) {
        inMemStringOvfPages->saveToFile();
    }
}

bool HashIndex::lookup(uint8_t* key, node_offset_t& result) {
    auto slotId = calculateSlotIdForHash(keyHashFunc(key));
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
        bool foundKey = equalsFuncInRead(key, keyInEntry, stringOvfPages.get());
        if (foundKey) {
            result =
                *(node_offset_t*)(keyInEntry + Types::getDataTypeSize(indexHeader.keyDataType));
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
        primaryPages[getPageIdForSlot(slotId)]->data, getSlotIdInPageForSlot(slotId)));
}

uint8_t* HashIndex::getOvfSlotFromOvfPages(uint64_t slotId) const {
    return const_cast<uint8_t*>(
        getSlotInAPage(ovfPages[getPageIdForSlot(slotId)]->data, getSlotIdInPageForSlot(slotId)));
}

uint64_t HashIndex::reserveOvfSlot() {
    if (indexHeader.numOvfSlots % indexHeader.numSlotsPerPage == 0) {
        ovfPages.emplace_back(memoryManager->allocateBlock(true));
        indexHeader.numOvfPages++;
    }
    return indexHeader.numOvfSlots++;
}

} // namespace storage
} // namespace graphflow
