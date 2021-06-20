#include "src/storage/include/index/overflow_pages_manager.h"

namespace graphflow {
namespace storage {

OverflowPagesManager::OverflowPagesManager(const string& indexPath, MemoryManager& memoryManager)
    : memoryManager{memoryManager} {
    auto filePath = FileUtils::joinPath(indexPath, OVERFLOW_FILE_NAME);
    // Open index files
    bool isIndexFileExist = true;
    int flags = O_RDWR;
    if (!FileUtils::fileExists(filePath)) {
        flags |= O_CREAT;
        isIndexFileExist = false;
    }
    fileHandle = make_unique<FileHandle>(filePath, flags);
    // Cache the first page of overflow index file
    getMemoryBlock(0);
    if (isIndexFileExist) {
        deSerOverflowPagesAllocationBitsets();
    }
}

uint8_t* OverflowPagesManager::getMemoryBlock(uint64_t blockId) {
    if (memoryBlocks.find(blockId) == memoryBlocks.end()) {
        // Allocate a memory block for the page, if failed, clean up allocated memoryBlocks and
        // throw exception.
        try {
            memoryBlocks[blockId] = memoryManager.allocateBlock(PAGE_SIZE, true /* initialize */);
        } catch (const invalid_argument& exception) {
            string exceptionMsg = exception.what();
            throw invalid_argument(
                "Cannot allocate block from MemoryManager. Exception caught: " + exceptionMsg);
        }
        if (blockId < fileHandle->numPages) {
            // Read from the overflow index file if the page exists on disk.
            fileHandle->readPage(memoryBlocks[blockId]->data, blockId);
        }
    }
    return memoryBlocks[blockId]->data;
}

void OverflowPagesManager::flush() {
    serOverflowPagesAllocationBitsets();
    for (auto& block : memoryBlocks) {
        fileHandle->writePage(block.second->data, block.first);
    }
}

uint64_t OverflowPagesManager::allocateFreeOverflowPage() {
    uint64_t pageGroupId = 0;
    uint64_t freeOverflowPageId = 0;
    while (freeOverflowPageId == 0) {
        if (pageGroupId >= overflowPagesAllocationBitsets.size()) {
            // Allocate a new page group.
            auto newPageGroupBitset = bitset<NUM_PAGES_PER_PAGE_GROUP>();
            newPageGroupBitset[0] = true;
            pageGroupId = overflowPagesAllocationBitsets.size();
            overflowPagesAllocationBitsets.push_back(newPageGroupBitset);
            freeOverflowPageId = pageGroupId * NUM_PAGES_PER_PAGE_GROUP + 1;
        }
        auto& pageGroupBitset = overflowPagesAllocationBitsets[pageGroupId];
        if (!pageGroupBitset.all()) {
            // Skip the first page for bitset in each page group.
            for (auto pagePos = 1u; pagePos < pageGroupBitset.size(); pagePos++) {
                if (!pageGroupBitset[pagePos]) {
                    freeOverflowPageId = pageGroupId * NUM_PAGES_PER_PAGE_GROUP + pagePos;
                    overflowPagesAllocationBitsets[pageGroupId][pagePos] = true;
                    break;
                }
            }
        } else {
            pageGroupId++;
        }
    }
    return freeOverflowPageId;
}

void OverflowPagesManager::serOverflowPageHeader(uint8_t* block, vector<bool>& bitset) {
    for (auto i = 0ul; i < bitset.size(); i++) {
        block[i >> 3] = block[i >> 3] | (bitset[i] << (i & 7));
    }
}

vector<bool> OverflowPagesManager::deSerOverflowPageHeader(const uint8_t* block, uint64_t numBits) {
    vector<bool> bitset(numBits);
    for (auto i = 0ul; i < bitset.size(); i++) {
        bitset[i] = (block[i >> 3] >> (i & 7)) & 1;
    }
    return bitset;
}

void OverflowPagesManager::serOverflowPagesAllocationBitsets() {
    for (auto pageGroupId = 0u; pageGroupId < overflowPagesAllocationBitsets.size();
         pageGroupId++) {
        uint8_t* block = getMemoryBlock(pageGroupId * NUM_PAGES_PER_PAGE_GROUP);
        for (auto pageInGroupId = 0ul; pageInGroupId < NUM_PAGES_PER_PAGE_GROUP; pageInGroupId++) {
            block[pageInGroupId >> 3] =
                block[pageInGroupId >> 3] |
                (overflowPagesAllocationBitsets[pageGroupId][pageInGroupId] << (pageInGroupId & 7));
        }
    }
}

void OverflowPagesManager::deSerOverflowPagesAllocationBitsets() {
    auto numPageGroups = fileHandle->numPages / NUM_PAGES_PER_PAGE_GROUP;
    for (auto pageGroupId = 0ul; pageGroupId < numPageGroups; pageGroupId++) {
        bitset<NUM_PAGES_PER_PAGE_GROUP> pageGroupBitset;
        uint8_t* block = getMemoryBlock(pageGroupId * NUM_PAGES_PER_PAGE_GROUP);
        for (auto pageInGroupId = 0ul; pageInGroupId < NUM_PAGES_PER_PAGE_GROUP; pageInGroupId++) {
            pageGroupBitset[pageInGroupId] = (block[pageInGroupId >> 3] >> (pageInGroupId & 7)) & 1;
        }
        overflowPagesAllocationBitsets.push_back(pageGroupBitset);
    }
}

} // namespace storage
} // namespace graphflow
