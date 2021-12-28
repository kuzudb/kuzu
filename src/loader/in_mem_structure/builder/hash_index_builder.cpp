#include "src/loader/include/in_mem_structure/builder/hash_index_builder.h"

#include <cstring>

#include "src/common/include/operations/hash_operations.h"

using namespace std;

namespace graphflow {
namespace storage {

HashIndexBuilder::HashIndexBuilder(const string& fName, DataType keyDataType)
    : fName{fName}, indexHeader{keyDataType}, logger(LoggerUtils::getOrCreateSpdLogger("storage")) {
    assert(keyDataType == INT64 || keyDataType == STRING);
    // When the Hash index is initialized without the BufferManager, it is opened for only writing.
    // The default for this configuration, has 2 slots and a single primary page.
    primaryPages.reserve(1);
    primaryPages.emplace_back(HashIndexUtils::getNewPage());
    // We intend ovfPages to start dispensing slots from ovfSlotId 1 because default values of
    // ovfSlotId in slot headers is 0 which we reserve for null.
    ovfPages.reserve(1);
    ovfPages.emplace_back(HashIndexUtils::getNewPage());
    if (keyDataType == STRING) {
        inMemStringOvfPages = make_unique<InMemStringOverflowPages>(
            StringOverflowPages::getStringOverflowPagesFName(fName));
    }
}

void HashIndexBuilder::bulkReserve(uint32_t numEntries) {
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
        primaryPages.emplace_back(HashIndexUtils::getNewPage());
    }
}

// First compute the hash, then check if key already exist, finally insert non-existing key.
// For now, we don't support re-write of key. Existing key will not be inserted.
// Return: true, insertion succeeds. false, insertion failed due to that the key already exists.
bool HashIndexBuilder::insert(uint8_t* key, uint64_t value) {
    auto slot = getSlotFromPrimaryPages(HashIndexUtils::calculateSlotIdForHash(
        indexHeader, HashIndexUtils::hashFunc(indexHeader, key)));
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

void HashIndexBuilder::putNewEntryInSlotAndUpdateHeader(
    uint8_t* slot, uint8_t* key, node_offset_t value) {
    auto slotHeader = reinterpret_cast<SlotHeader*>(slot);
    auto entry = HashIndexUtils::getEntryInSlot(indexHeader, slot, slotHeader->numEntries);
    insertKey(key, entry);
    entry += TypeUtils::getDataTypeSize(indexHeader.keyDataType);
    memcpy(entry, &value, sizeof(node_offset_t));
    indexHeader.numEntries += 1;
    slotHeader->numEntries++;
}

void HashIndexBuilder::insertKey(uint8_t* key, uint8_t* entry) {
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

bool HashIndexBuilder::notExistsInSlot(uint8_t* slot, uint8_t* key) {
    auto slotHeader = reinterpret_cast<SlotHeader*>(slot);
    for (auto entryPos = 0u; entryPos < slotHeader->numEntries; entryPos++) {
        auto keyInEntry = HashIndexUtils::getEntryInSlot(indexHeader, slot, entryPos);
        bool foundKey = false;
        switch (indexHeader.keyDataType) {
        case INT64:
            foundKey = HashIndexUtils::equalsInt64(key, keyInEntry);
            break;
        case STRING: {
            auto keyInEntryString = (gf_string_t*)keyInEntry;
            if (HashIndexUtils::likelyEqualsString(key, keyInEntryString)) {
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

void HashIndexBuilder::saveToDisk() {
    auto fileInfo = FileUtils::openFile(fName, O_WRONLY | O_CREAT);
    unique_ptr<uint8_t[]> buffer(HashIndexUtils::getNewPage());
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
    if (indexHeader.keyDataType == STRING) {
        inMemStringOvfPages->saveToFile();
    }
}

uint8_t* HashIndexBuilder::getSlotFromPrimaryPages(uint64_t slotId) const {
    return const_cast<uint8_t*>(HashIndexUtils::getSlotInAPage(indexHeader,
        primaryPages[HashIndexUtils::getPageIdForSlot(indexHeader, slotId)].get(),
        HashIndexUtils::getSlotIdInPageForSlot(indexHeader, slotId)));
}

uint8_t* HashIndexBuilder::getOvfSlotFromOvfPages(uint64_t slotId) const {
    return const_cast<uint8_t*>(HashIndexUtils::getSlotInAPage(indexHeader,
        ovfPages[HashIndexUtils::getPageIdForSlot(indexHeader, slotId)].get(),
        HashIndexUtils::getSlotIdInPageForSlot(indexHeader, slotId)));
}

uint64_t HashIndexBuilder::reserveOvfSlot() {
    if (indexHeader.numOvfSlots % indexHeader.numSlotsPerPage == 0) {
        ovfPages.emplace_back(HashIndexUtils::getNewPage());
        indexHeader.numOvfPages++;
    }
    return indexHeader.numOvfSlots++;
}

} // namespace storage
} // namespace graphflow
