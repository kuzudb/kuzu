#include "src/loader/include/utils.h"

#include <string.h>

#include <unordered_map>

#include "spdlog/spdlog.h"

#include "src/common/include/configs.h"
#include "src/storage/include/data_structure/lists/lists.h"

namespace graphflow {
namespace loader {

NodeIDMap::~NodeIDMap() {
    for (auto& charArray : nodeIDToOffsetMap) {
        delete[] charArray.first;
    }
}

void NodeIDMap::set(const char* nodeID, node_offset_t nodeOffset) {
    auto len = strlen(nodeID);
    auto nodeIDcopy = new char[len + 1];
    memcpy(nodeIDcopy, nodeID, len);
    nodeIDcopy[len] = 0;
    offsetToNodeIDMap[nodeOffset] = nodeIDcopy;
}

node_offset_t NodeIDMap::get(const char* nodeID) {
    return nodeIDToOffsetMap.at(nodeID);
}

void ListsLoaderHelper::calculateListHeadersTask(node_offset_t numNodeOffsets,
    uint32_t numElementsPerPage, listSizes_t* listSizes, ListHeaders* listHeaders,
    const shared_ptr<spdlog::logger>& logger) {
    logger->trace("Start: ListHeaders={0:p}", (void*)listHeaders);
    auto numChunks = numNodeOffsets / BaseLists::LISTS_CHUNK_SIZE;
    if (0 != numNodeOffsets % BaseLists::LISTS_CHUNK_SIZE) {
        numChunks++;
    }
    node_offset_t nodeOffset = 0u;
    uint64_t lAdjListsIdx = 0u;
    for (auto chunkId = 0u; chunkId < numChunks; chunkId++) {
        auto csrOffset = 0u;
        auto lastNodeOffsetInChunk = min(nodeOffset + BaseLists::LISTS_CHUNK_SIZE, numNodeOffsets);
        for (auto i = nodeOffset; i < lastNodeOffsetInChunk; i++) {
            auto numElementsInList = (*listSizes)[nodeOffset].load(memory_order_relaxed);
            uint32_t header;
            if (numElementsInList >= numElementsPerPage) {
                header = 0x80000000 + (lAdjListsIdx++ & 0x7fffffff);
            } else {
                header = ((csrOffset & 0xfffff) << 11) + (numElementsInList & 0x7ff);
                csrOffset += numElementsInList;
            }
            (*listHeaders).headers[nodeOffset] = header;
            nodeOffset++;
        }
    }
    logger->trace("End: adjListHeaders={0:p}", (void*)listHeaders);
}

void ListsLoaderHelper::calculateListsMetadataTask(uint64_t numNodeOffsets, uint32_t numPerPage,
    listSizes_t* listSizes, ListHeaders* listHeaders, ListsMetadata* listsMetadata,
    const shared_ptr<spdlog::logger>& logger) {
    logger->trace("Start: listsMetadata={0:p} adjListHeaders={1:p}", (void*)listsMetadata,
        (void*)listHeaders);
    auto globalPageId = 0u;
    auto numChunks = numNodeOffsets / BaseLists::LISTS_CHUNK_SIZE;
    if (0 != numNodeOffsets % BaseLists::LISTS_CHUNK_SIZE) {
        numChunks++;
    }
    (*listsMetadata).initChunkPageLists(numChunks);
    node_offset_t nodeOffset = 0u;
    auto largeListIdx = 0u;
    for (auto chunkId = 0u; chunkId < numChunks; chunkId++) {
        auto lastNodeOffsetInChunk = min(nodeOffset + BaseLists::LISTS_CHUNK_SIZE, numNodeOffsets);
        for (auto i = nodeOffset; i < lastNodeOffsetInChunk; i++) {
            if (ListHeaders::isALargeList(listHeaders->headers[nodeOffset])) {
                largeListIdx++;
            }
            nodeOffset++;
        }
    }
    (*listsMetadata).initLargeListPageLists(largeListIdx);
    nodeOffset = 0u;
    largeListIdx = 0u;
    for (auto chunkId = 0u; chunkId < numChunks; chunkId++) {
        auto numPages = 0u, offsetInPage = 0u;
        auto lastNodeOffsetInChunk = min(nodeOffset + BaseLists::LISTS_CHUNK_SIZE, numNodeOffsets);
        for (auto i = nodeOffset; i < lastNodeOffsetInChunk; i++) {
            auto numElementsInList = (*listSizes)[nodeOffset].load(memory_order_relaxed);
            if (ListHeaders::isALargeList(listHeaders->headers[nodeOffset])) {
                auto numPagesForLargeList = numElementsInList / numPerPage;
                if (0 != numElementsInList % numPerPage) {
                    numPagesForLargeList++;
                }
                (*listsMetadata)
                    .populateLargeListPageList(
                        largeListIdx, numPagesForLargeList, numElementsInList, globalPageId);
                globalPageId += numPagesForLargeList;
                largeListIdx++;
            } else {
                while (numElementsInList + offsetInPage > numPerPage) {
                    numElementsInList -= (numPerPage - offsetInPage);
                    numPages++;
                    offsetInPage = 0;
                }
                offsetInPage += numElementsInList;
            }
            nodeOffset++;
        }
        if (0 == offsetInPage) {
            (*listsMetadata).populateChunkPageList(chunkId, numPages, globalPageId);
            globalPageId += numPages;
        } else {
            (*listsMetadata).populateChunkPageList(chunkId, numPages + 1, globalPageId);
            globalPageId += numPages + 1;
        }
    }
    listsMetadata->numPages = globalPageId;
    logger->trace(
        "End: listsMetadata={0:p} listHeaders={1:p}", (void*)listsMetadata, (void*)listHeaders);
}

void ListsLoaderHelper::calculatePageCursor(const uint32_t& header, const uint64_t& reversePos,
    const uint8_t& numBytesPerElement, const node_offset_t& nodeOffset, PageCursor& cursor,
    ListsMetadata& metadata) {
    auto numElementsInAPage = PAGE_SIZE / numBytesPerElement;
    if (ListHeaders::isALargeList(header)) {
        auto lAdjListIdx = ListHeaders::getLargeListIdx(header);
        auto pos = metadata.getNumElementsInLargeLists(lAdjListIdx) - reversePos;
        cursor.offset = numBytesPerElement * (pos % numElementsInAPage);
        cursor.idx =
            metadata.getPageIdxFromALargeListPageList(lAdjListIdx, (pos / numElementsInAPage));
    } else {
        auto chunkId = nodeOffset / BaseLists::LISTS_CHUNK_SIZE;
        auto csrOffset = ListHeaders::getSmallListCSROffset(header);
        auto pos = ListHeaders::getSmallListLen(header) - reversePos;
        cursor.idx =
            metadata.getPageIdxFromAChunkPageList(chunkId, (csrOffset + pos) / numElementsInAPage);
        cursor.offset = numBytesPerElement * ((csrOffset + pos) % numElementsInAPage);
    }
}

} // namespace loader
} // namespace graphflow
