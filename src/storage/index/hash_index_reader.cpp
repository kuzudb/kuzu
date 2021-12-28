#include "src/storage/include/index/hash_index_reader.h"

#include <cstring>

#include "src/common/include/operations/hash_operations.h"

using namespace std;

namespace graphflow {
namespace storage {

HashIndexReader::HashIndexReader(const string& fName, BufferManager& bufferManager, bool isInMemory)
    : fName{fName},
      logger(LoggerUtils::getOrCreateSpdLogger("storage")), bufferManager{&bufferManager} {
    fileHandle = make_unique<FileHandle>(fName, isInMemory);
    unique_ptr<uint8_t[]> buffer{HashIndexUtils::getNewPage()};
    fileHandle->readPage(buffer.get(), 0);
    indexHeader = *((HashIndexHeader*)buffer.get());
    if (indexHeader.keyDataType == STRING) {
        stringOvfPages = make_unique<StringOverflowPages>(fName, bufferManager, isInMemory);
    }
}

bool HashIndexReader::lookup(uint8_t* key, node_offset_t& result, BufferManagerMetrics& metrics) {
    auto slotId = HashIndexUtils::calculateSlotIdForHash(
        indexHeader, HashIndexUtils::hashFunc(indexHeader, key));
    auto pageId = HashIndexUtils::getPageIdForSlot(indexHeader, slotId);
    auto slotIdInPage = HashIndexUtils::getSlotIdInPageForSlot(indexHeader, slotId);
    auto physicalPageId = pageId + 1;
    auto page = bufferManager->pin(*fileHandle, physicalPageId, metrics);
    auto slot = HashIndexUtils::getSlotInAPage(indexHeader, page, slotIdInPage);
    auto foundKey = lookupInSlot(slot, key, result, metrics);
    if (foundKey) {
        bufferManager->unpin(*fileHandle, physicalPageId);
        return true;
    }
    auto slotHeader = reinterpret_cast<SlotHeader*>(const_cast<uint8_t*>(slot));
    while (slotHeader->nextOvfSlotId != 0) {
        pageId = HashIndexUtils::getPageIdForSlot(indexHeader, slotHeader->nextOvfSlotId);
        slotIdInPage =
            HashIndexUtils::getSlotIdInPageForSlot(indexHeader, slotHeader->nextOvfSlotId);
        bufferManager->unpin(*fileHandle, physicalPageId);
        physicalPageId = 1 + indexHeader.numPrimaryPages + pageId;
        page = bufferManager->pin(*fileHandle, physicalPageId, metrics);
        slot = HashIndexUtils::getSlotInAPage(indexHeader, page, slotIdInPage);
        foundKey = lookupInSlot(slot, key, result, metrics);
        if (foundKey) {
            bufferManager->unpin(*fileHandle, physicalPageId);
            return true;
        }
        slotHeader = reinterpret_cast<SlotHeader*>(const_cast<uint8_t*>(slot));
    }
    bufferManager->unpin(*fileHandle, physicalPageId);
    return false;
}

bool HashIndexReader::lookupInSlot(
    const uint8_t* slot, uint8_t* key, node_offset_t& result, BufferManagerMetrics& metrics) const {
    auto slotHeader = reinterpret_cast<SlotHeader*>(const_cast<uint8_t*>(slot));
    for (auto entryPos = 0u; entryPos < slotHeader->numEntries; entryPos++) {
        auto keyInEntry =
            HashIndexUtils::getEntryInSlot(indexHeader, const_cast<uint8_t*>(slot), entryPos);
        bool foundKey = false;
        switch (indexHeader.keyDataType) {
        case INT64:
            foundKey = HashIndexUtils::equalsInt64(key, keyInEntry);
            break;
        case STRING: {
            auto keyInEntryString = (gf_string_t*)keyInEntry;
            if (HashIndexUtils::likelyEqualsString(key, keyInEntryString)) {
                auto entryKeyString = stringOvfPages->readString(*keyInEntryString, metrics);
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

} // namespace storage
} // namespace graphflow
