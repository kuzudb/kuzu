#include "src/storage/include/structures/lists.h"

namespace graphflow {
namespace storage {

void BaseLists::readValues(const nodeID_t& nodeID, const shared_ptr<ValueVector>& valueVector,
    uint64_t& listLen, const unique_ptr<ColumnOrListsHandle>& handle, uint32_t maxElementsToRead) {
    auto header = headers->getHeader(nodeID.offset);
    if (handle->hasMoreToRead() || AdjListHeaders::isALargeAdjList(header)) {
        readFromLargeList(nodeID, valueVector, listLen, handle, header, maxElementsToRead);
    } else {
        readSmallList(nodeID, valueVector, listLen, handle, header);
    }
}

void BaseLists::readSmallList(const nodeID_t& nodeID, const shared_ptr<ValueVector>& valueVector,
    uint64_t& listLen, const unique_ptr<ColumnOrListsHandle>& handle, uint32_t header) {
    if (handle->getIsAdjListsHandle()) {
        listLen = AdjListHeaders::getAdjListLen(header);
    }
    auto csrOffset = AdjListHeaders::getCSROffset(header);
    auto pageOffset = getPageOffset(csrOffset);
    uint32_t chunkIdx = nodeID.offset / LISTS_CHUNK_SIZE;
    auto pageIdx = getPageIdx(csrOffset);
    auto sizeLeftToCopy = listLen * elementSize;
    if (pageOffset + sizeLeftToCopy > PAGE_SIZE) {
        readBySequentialCopy(valueVector, handle, sizeLeftToCopy, pageIdx, pageOffset,
            metadata.getPageMapperForChunkIdx(chunkIdx));
    } else {
        auto physicalPageIdx = metadata.getPageIdxFromChunkPagesMap(chunkIdx, pageIdx);
        readBySettingFrame(valueVector, handle, physicalPageIdx, pageOffset);
    }
}

void BaseLists::readFromLargeList(const nodeID_t& nodeID,
    const shared_ptr<ValueVector>& valueVector, uint64_t& listLen,
    const unique_ptr<ColumnOrListsHandle>& handle, uint32_t header, uint32_t maxElementsToRead) {
    auto largeListIdx = AdjListHeaders::getLargeAdjListIdx(header);
    auto listSyncer = handle->getListSyncer();
    auto csrOffset = listSyncer->getStartIdx();
    auto pageIdx = getPageIdx(csrOffset);
    auto pageOffset = getPageOffset(csrOffset);
    auto sizeLeftToCopy = elementSize * listLen;
    if (pageOffset + sizeLeftToCopy > PAGE_SIZE) {
        readBySequentialCopy(valueVector, handle, sizeLeftToCopy, pageIdx, pageOffset,
            metadata.getPageMapperForLargeListIdx(largeListIdx));
    } else {
        auto physicalPageIdx = metadata.getPageIdxFromLargeListPagesMap(largeListIdx, pageIdx);
        readBySettingFrame(valueVector, handle, physicalPageIdx, pageOffset);
    }
}

void Lists<nodeID_t>::readFromLargeList(const nodeID_t& nodeID,
    const shared_ptr<ValueVector>& valueVector, uint64_t& listLen,
    const unique_ptr<ColumnOrListsHandle>& handle, uint32_t header, uint32_t maxElementsToRead) {
    auto largeListIdx = AdjListHeaders::getLargeAdjListIdx(header);
    auto numElements = metadata.getNumElementsInLargeLists(largeListIdx);
    auto listSyncer = handle->getListSyncer();
    uint64_t csrOffset;
    if (!handle->hasMoreToRead()) {
        listSyncer->init(numElements);
        csrOffset = 0;
    } else {
        csrOffset = listSyncer->getEndIdx();
    }
    auto pageIdx = getPageIdx(csrOffset);
    auto pageOffset = getPageOffset(csrOffset);
    listLen = min((uint32_t)(numElements - csrOffset),
        min(numElementsPerPage - pageOffset, maxElementsToRead));
    auto physicalPageIdx = metadata.getPageIdxFromLargeListPagesMap(largeListIdx, pageIdx);
    readBySettingFrame(valueVector, handle, physicalPageIdx, pageOffset);
    listSyncer->set(csrOffset, listLen);
}

} // namespace storage
} // namespace graphflow
