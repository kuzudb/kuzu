#include "src/storage/include/storage_structure/lists/lists_metadata.h"

#include "src/common/include/utils.h"
#include "src/storage/include/storage_structure/lists/utils.h"

namespace graphflow {
namespace storage {

ListsMetadata::ListsMetadata() {
    logger = LoggerUtils::getOrCreateSpdLogger("storage");
}

ListsMetadata::ListsMetadata(const string& listBaseFName) : ListsMetadata() {
    readFromDisk(listBaseFName);
    logger->trace("ListsMetadata: #Chunks {}, #largeLists {}", numChunks, numLargeLists);
};

void ListsMetadata::saveToDisk(const string& fName) {
    auto metadataBasePath = fName + ".metadata";
    saveListOfIntsToFile(metadataBasePath + CHUNK_PAGE_LIST_HEAD_IDX_MAP_SUFFIX,
        chunkToPageListHeadIdxMap, numChunks + 1);
    saveListOfIntsToFile(
        metadataBasePath + CHUNK_PAGE_LISTS_SUFFIX, chunkPageLists, chunkPageListsCapacity);
    saveListOfIntsToFile(metadataBasePath + LARGE_LISTS_PAGE_LIST_HEAD_IDX_MAP_SUFFIX,
        largeListIdxToPageListHeadIdxMap, (2 * numLargeLists) + 1);
    saveListOfIntsToFile(metadataBasePath + LARGE_LISTS_PAGE_LISTS_SUFFIX, largeListPageLists,
        largeListPageListsCapacity);

    // put numPages in .metadata file.
    if (0 == metadataBasePath.length()) {
        throw invalid_argument("ListsMetadata: Empty filename");
    }
    uint32_t f = open(metadataBasePath.c_str(), O_WRONLY | O_CREAT, 0666);
    if (-1u == f) {
        throw invalid_argument("ListsMetadata: Cannot create file: " + metadataBasePath);
    }
    auto bytesToWrite = sizeof(uint32_t);
    if (bytesToWrite != write(f, &numPages, bytesToWrite)) {
        throw invalid_argument("ListsMetadata: Cannot write in file: " + metadataBasePath);
    }
    close(f);
}

void ListsMetadata::readFromDisk(const string& fName) {
    auto metadataBasePath = fName + ".metadata";
    auto listSize = readListOfIntsFromFile(
        chunkToPageListHeadIdxMap, metadataBasePath + CHUNK_PAGE_LIST_HEAD_IDX_MAP_SUFFIX);
    this->numChunks = listSize - 1;
    listSize = readListOfIntsFromFile(chunkPageLists, metadataBasePath + CHUNK_PAGE_LISTS_SUFFIX);
    this->chunkPageListsCapacity = listSize;
    listSize = readListOfIntsFromFile(largeListIdxToPageListHeadIdxMap,
        metadataBasePath + LARGE_LISTS_PAGE_LIST_HEAD_IDX_MAP_SUFFIX);
    this->numLargeLists = (listSize - 1) / 2;
    listSize = readListOfIntsFromFile(
        largeListPageLists, metadataBasePath + LARGE_LISTS_PAGE_LISTS_SUFFIX);
    this->largeListPageListsCapacity = listSize;

    // read numPages from .metadata file.
    if (0 == metadataBasePath.length()) {
        throw invalid_argument("ListsMetadata: Empty filename");
    }
    uint32_t f = open(metadataBasePath.c_str(), O_RDONLY);
    auto bytesToRead = sizeof(uint32_t);
    if (bytesToRead != read(f, &numPages, bytesToRead)) {
        throw invalid_argument("ListsMetadata: Cannot read from file.");
    }
    close(f);
}

uint64_t ListsMetadata::getPageIdxFromAPageList(
    uint32_t* pageLists, uint32_t pageListHeadIdx, uint32_t pageIdx) {
    auto pageListGroupHeadIdx = pageListHeadIdx;
    while (PAGE_LIST_GROUP_SIZE <= pageIdx) {
        pageListGroupHeadIdx = pageLists[pageListGroupHeadIdx + PAGE_LIST_GROUP_SIZE];
        pageIdx -= PAGE_LIST_GROUP_SIZE;
    }
    return pageLists[pageListGroupHeadIdx + pageIdx];
}

void ListsMetadata::initChunkPageLists(uint32_t numChunks) {
    this->numChunks = numChunks;
    chunkToPageListHeadIdxMap = make_unique<uint32_t[]>(numChunks + 1);
    // Make the headIdx of the first chunk 0.
    chunkToPageListHeadIdxMap[0] = 0;
    // initialize pageLists blob with size 100.
    chunkPageListsCapacity = 100;
    chunkPageLists = make_unique<uint32_t[]>(chunkPageListsCapacity);
}

void ListsMetadata::initLargeListPageLists(uint32_t numLargeLists) {
    this->numLargeLists = numLargeLists;
    // For each largeList, we store the PageListHeadIdx in pageLists and also the number of elements
    // in the large list.
    largeListIdxToPageListHeadIdxMap = make_unique<uint32_t[]>((2 * numLargeLists) + 1);
    // Make the headIdx of the first large list 0.
    largeListIdxToPageListHeadIdxMap[0] = 0;
    // initialize pageLists blob with size 100.
    largeListPageListsCapacity = 100;
    largeListPageLists = make_unique<uint32_t[]>(largeListPageListsCapacity);
}

void ListsMetadata::populateChunkPageList(
    uint32_t chunkId, uint32_t numPages, uint32_t startPageId) {
    auto pageListHeadIdx = chunkToPageListHeadIdxMap[chunkId];
    auto pageListTail = enumeratePageIdsInAPageList(
        chunkPageLists, chunkPageListsCapacity, pageListHeadIdx, numPages, startPageId);
    chunkToPageListHeadIdxMap[chunkId + 1] = pageListTail;
}

void ListsMetadata::populateLargeListPageList(
    uint32_t largeListIdx, uint32_t numPages, uint32_t numElements, uint32_t startPageId) {
    auto offsetInMap = 2 * largeListIdx;
    auto pageListHeadIdx = largeListIdxToPageListHeadIdxMap[offsetInMap];
    largeListIdxToPageListHeadIdxMap[offsetInMap + 1] = numElements;
    auto pageListTail = enumeratePageIdsInAPageList(
        largeListPageLists, largeListPageListsCapacity, pageListHeadIdx, numPages, startPageId);
    largeListIdxToPageListHeadIdxMap[offsetInMap + 2] = pageListTail;
}

uint32_t ListsMetadata::enumeratePageIdsInAPageList(unique_ptr<uint32_t[]>& pageLists,
    uint64_t& pageListsCapacity, uint32_t pageListHeadIdx, uint32_t numPages,
    uint32_t startPageId) {
    // calculate the number of pageListGroups to accommodate the pageList completely.
    auto numPageListGroups = numPages / PAGE_LIST_GROUP_SIZE;
    if (0 != numPages % PAGE_LIST_GROUP_SIZE) {
        numPageListGroups++;
    }
    // During the initial allocation, we allocate all the pageListGroups of a pageList contiguously.
    // pageListTailIdx is the id in the pageLists blob where the pageList ends and the next
    // pageLists should start.
    auto pageListTailIdx = pageListHeadIdx + ((PAGE_LIST_GROUP_SIZE + 1) * numPageListGroups);
    increasePageListsCapacityIfNeeded(pageLists, pageListsCapacity, pageListTailIdx);
    for (auto i = 0u; i < numPageListGroups; i++) {
        auto numPagesInThisGroup = min(PAGE_LIST_GROUP_SIZE, numPages);
        for (auto j = 0u; j < numPagesInThisGroup; j++) {
            pageLists[pageListHeadIdx + j] = startPageId++;
        }
        // if there are empty spots in the pageListGroup, put -1 in them.
        for (auto j = numPagesInThisGroup; j < PAGE_LIST_GROUP_SIZE; j++) {
            pageLists[pageListHeadIdx + j] = -1;
        }
        numPages -= numPagesInThisGroup;
        pageListHeadIdx += PAGE_LIST_GROUP_SIZE;
        // store the id to the next pageListGroup, if exists, other -1.
        pageLists[pageListHeadIdx] = (0 == numPages) ? -1 : 1 + pageListHeadIdx;
        pageListHeadIdx++;
    }
    return pageListTailIdx;
}

void ListsMetadata::increasePageListsCapacityIfNeeded(
    unique_ptr<uint32_t[]>& pageLists, uint64_t& pageListsCapacity, uint32_t requiredCapacity) {
    auto growth_factor = 1.2;
    if (pageListsCapacity <= requiredCapacity) {
        auto newCapacity = pageListsCapacity;
        while (newCapacity <= requiredCapacity) {
            newCapacity *= growth_factor;
        }
        auto newMemBlock = new uint32_t[newCapacity];
        memcpy(newMemBlock, pageLists.get(), sizeof(uint32_t) * pageListsCapacity);
        pageLists.reset(newMemBlock);
        pageListsCapacity = newCapacity;
    }
}

} // namespace storage
} // namespace graphflow
