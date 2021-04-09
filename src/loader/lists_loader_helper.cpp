#include "src/loader/include/lists_loader_helper.h"

#include "src/storage/include/structures/lists.h"

namespace graphflow {
namespace loader {

void ListsLoaderHelper::calculateListHeadersTask(node_offset_t numNodeOffsets, uint32_t numPerPage,
    listSizes_t* listSizes, ListHeaders* listHeaders, shared_ptr<spdlog::logger> logger) {
    logger->trace("Start: ListHeaders={0:p}", (void*)listHeaders);
    auto numChunks = numNodeOffsets / BaseLists::LISTS_CHUNK_SIZE;
    if (0 != numNodeOffsets % BaseLists::LISTS_CHUNK_SIZE) {
        numChunks++;
    }
    node_offset_t nodeOffset = 0u;
    uint64_t lAdjListsIdx = 0u;
    for (auto chunkId = 0u; chunkId < numChunks; chunkId++) {
        auto csrOffset = 0u;
        auto numElementsInChunk = min(nodeOffset + BaseLists::LISTS_CHUNK_SIZE, numNodeOffsets);
        for (auto i = nodeOffset; i < numElementsInChunk; i++) {
            auto relCount = (*listSizes)[nodeOffset].load(memory_order_relaxed);
            uint32_t header;
            if (relCount >= numPerPage) {
                header = 0x80000000 + (lAdjListsIdx++ & 0x7fffffff);
            } else {
                header = ((csrOffset & 0xfffff) << 11) + (relCount & 0x7ff);
                csrOffset += relCount;
            }
            (*listHeaders).headers[nodeOffset] = header;
            nodeOffset++;
        }
    }
    logger->trace("End: adjListHeaders={0:p}", (void*)listHeaders);
}

void ListsLoaderHelper::calculateListsMetadataTask(uint64_t numNodeOffsets, uint32_t numPerPage,
    listSizes_t* listSizes, ListHeaders* listHeaders, ListsMetadata* listsMetadata,
    shared_ptr<spdlog::logger> logger) {
    logger->trace("Start: listsMetadata={0:p} adjListHeaders={1:p}", (void*)listsMetadata,
        (void*)listHeaders);
    auto globalPageId = 0u;
    auto numChunks = numNodeOffsets / BaseLists::LISTS_CHUNK_SIZE;
    if (0 != numNodeOffsets % BaseLists::LISTS_CHUNK_SIZE) {
        numChunks++;
    }
    (*listsMetadata).chunksPagesMap.resize(numChunks);
    node_offset_t nodeOffset = 0u;
    auto lAdjListsIdx = 0u;
    for (auto chunkId = 0u; chunkId < numChunks; chunkId++) {
        auto pageId = 0u, csrOffsetInPage = 0u;
        auto numElementsInChunk = min(nodeOffset + BaseLists::LISTS_CHUNK_SIZE, numNodeOffsets);
        for (auto i = nodeOffset; i < numElementsInChunk; i++) {
            auto relCount = (*listSizes)[nodeOffset].load(memory_order_relaxed);
            if (ListHeaders::isALargeList(listHeaders->headers[nodeOffset])) {
                (*listsMetadata).largeListsPagesMap.resize(lAdjListsIdx + 1);
                auto numPages = relCount / numPerPage;
                if (0 != relCount % numPerPage) {
                    numPages++;
                }
                (*listsMetadata).largeListsPagesMap[lAdjListsIdx].resize(numPages + 1);
                (*listsMetadata).largeListsPagesMap[lAdjListsIdx][0] = relCount;
                for (auto i = 1u; i <= numPages; i++) {
                    (*listsMetadata).largeListsPagesMap[lAdjListsIdx][i] = globalPageId++;
                }
                lAdjListsIdx++;
            } else {
                while (relCount + csrOffsetInPage > numPerPage) {
                    relCount -= (numPerPage - csrOffsetInPage);
                    pageId++;
                    csrOffsetInPage = 0;
                }
                csrOffsetInPage += relCount;
            }
            nodeOffset++;
        }
        if (pageId != 0 || csrOffsetInPage != 0) {
            (*listsMetadata).chunksPagesMap[chunkId].resize(pageId + 1);
            for (auto i = 0u; i < pageId + 1; i++) {
                (*listsMetadata).chunksPagesMap[chunkId][i] = globalPageId++;
            }
        }
    }
    (*listsMetadata).numPages = globalPageId;
    logger->trace(
        "End: listsMetadata={0:p} adjListHeaders={1:p}", (void*)listsMetadata, (void*)listHeaders);
}

void ListsLoaderHelper::calculatePageCursor(const uint32_t& header, const uint64_t& reversePos,
    const uint8_t& numBytesPerElement, const node_offset_t& nodeOffset, PageCursor& cursor,
    ListsMetadata& metadata) {
    auto numElementsInAPage = PAGE_SIZE / numBytesPerElement;
    if (ListHeaders::isALargeList(header)) {
        auto lAdjListIdx = ListHeaders::getLargeListIdx(header);
        auto pos = metadata.largeListsPagesMap[lAdjListIdx][0] - reversePos;
        cursor.offset = numBytesPerElement * (pos % numElementsInAPage);
        cursor.idx = metadata.largeListsPagesMap[lAdjListIdx][1 + (pos / numElementsInAPage)];
        return;
    }
    auto chunkId = nodeOffset / BaseLists::LISTS_CHUNK_SIZE;
    auto csrOffset = ListHeaders::getSmallListCSROffset(header);
    auto pos = ListHeaders::getSmallListLen(header) - reversePos;
    cursor.idx = metadata.chunksPagesMap[chunkId][(csrOffset + pos) / numElementsInAPage];
    cursor.offset = numBytesPerElement * ((csrOffset + pos) % numElementsInAPage);
}

} // namespace loader
} // namespace graphflow