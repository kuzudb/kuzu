#include "src/storage/include/structures/lists.h"

namespace graphflow {
namespace storage {

void BaseLists::readValues(const nodeID_t& nodeID, const shared_ptr<ValueVector>& valueVector,
    uint64_t& adjListLen, const unique_ptr<VectorFrameHandle>& handle) {
    auto header = headers->getHeader(nodeID.offset);
    if (!AdjListHeaders::isALargeAdjList(header)) {
        adjListLen = AdjListHeaders::getAdjListLen(header);
        auto csrOffsetInChunkPage = AdjListHeaders::getCSROffset(header) % numElementsPerPage;
        auto chunkIdx = nodeID.offset / LISTS_CHUNK_SIZE;
        auto pageIdxInChunk = AdjListHeaders::getCSROffset(header) / numElementsPerPage;
        if (csrOffsetInChunkPage + adjListLen > numElementsPerPage) {
            handle->isFrameBound = false;
            valueVector->reset();
            auto values = valueVector->getValues();
            auto pageOffset = csrOffsetInChunkPage * elementSize;
            auto sizeLeftToCopy = adjListLen * elementSize;
            while (sizeLeftToCopy) {
                auto pageIdx = metadata.getPageIdx(chunkIdx, pageIdxInChunk);
                uint64_t sizeToCopyInPage = min(PAGE_SIZE, sizeLeftToCopy) - pageOffset;
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
            valueVector->setValues((uint8_t*)frame);
        }
    } else {
        // TODO: Implement handling of large adjLists.
    }
}

} // namespace storage
} // namespace graphflow
