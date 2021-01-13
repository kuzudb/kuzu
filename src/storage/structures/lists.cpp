#include "src/storage/include/structures/lists.h"

#include <iostream>

namespace graphflow {
namespace storage {

void BaseLists::readValues(const nodeID_t& nodeID, const shared_ptr<ValueVector>& valueVector,
    uint64_t& adjListLen, const unique_ptr<VectorFrameHandle>& handle) {
    auto header = headers->getHeader(nodeID.offset);
    if (!AdjListHeaders::isALargeAdjList(header)) {
        adjListLen = AdjListHeaders::getAdjListLen(header);
        auto csrOffset = AdjListHeaders::getCSROffset(header);
        auto pageOffset = getPageOffset(csrOffset);
        auto chunkIdx = nodeID.offset / LISTS_CHUNK_SIZE;
        auto pageIdxInChunk = getPageIdx(csrOffset);
        auto sizeLeftToCopy = adjListLen * elementSize;
        if (pageOffset + sizeLeftToCopy > PAGE_SIZE) {
            handle->isFrameBound = false;
            valueVector->reset();
            auto values = valueVector->getValues();
            while (sizeLeftToCopy) {
                auto pageIdx = metadata.getPageIdx(chunkIdx, pageIdxInChunk);
                auto sizeToCopyInPage = min(PAGE_SIZE - pageOffset, sizeLeftToCopy);
                auto frame = bufferManager.pin(fileHandle, pageIdx);
                memcpy(values, frame + pageOffset, sizeToCopyInPage);
                bufferManager.unpin(fileHandle, pageIdx);
                values += sizeToCopyInPage;
                sizeLeftToCopy -= sizeToCopyInPage;
                pageOffset = 0;
                pageIdxInChunk++;
            }
        } else {
            handle->pageIdx = metadata.getPageIdx(chunkIdx, pageIdxInChunk);
            handle->isFrameBound = true;
            auto frame = bufferManager.pin(fileHandle, handle->pageIdx);
            valueVector->setValues((uint8_t*)frame + pageOffset);
        }
    } else {
        // TODO: Implement handling of large adjLists.
    }
}

} // namespace storage
} // namespace graphflow
