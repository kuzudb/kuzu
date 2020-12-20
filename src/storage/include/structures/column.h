#pragma once

#include <string.h>

#include <iostream>

#include <bits/unique_ptr.h>

#include "src/common/include/vector/node_vector.h"
#include "src/storage/include/buffer_manager.h"

namespace graphflow {
namespace storage {

struct VectorFrameHandle {
    uint32_t pageIdx;
    bool isFrameBound;
    VectorFrameHandle() : pageIdx(-1), isFrameBound(false){};
};

class Column {

public:
    Column(
        const string path, size_t elementSize, uint64_t numElements, BufferManager& bufferManager)
        : elementSize{elementSize}, numElementsPerPage{(uint32_t)(PAGE_SIZE / elementSize)},
          fileHandle{path, 1 + (numElements / numElementsPerPage)}, bufferManager{bufferManager} {};

    inline static uint64_t getPageIdx(node_offset_t nodeOffset, uint32_t numElementsPerPage) {
        return nodeOffset / numElementsPerPage;
    }

    inline static uint32_t getPageOffset(
        node_offset_t nodeOffset, uint32_t numElementsPerPage, size_t elementSize) {
        return (nodeOffset % numElementsPerPage) * elementSize;
    }

    void readValues(const shared_ptr<BaseNodeIDVector>& nodeIDVector,
        const shared_ptr<ValueVector>& valueVector, const uint64_t size,
        const unique_ptr<VectorFrameHandle>& handle) {
        if (nodeIDVector->isStoredSequentially()) {
            nodeID_t node;
            nodeIDVector->get(0, node);
            auto startOffset = node.offset;
            auto pageOffset = startOffset % numElementsPerPage;
            if (pageOffset + size <= numElementsPerPage) {
                handle->pageIdx = getPageIdx(startOffset, numElementsPerPage);
                handle->isFrameBound = true;
                auto frame = bufferManager.pin(fileHandle, handle->pageIdx);
                valueVector->setValues((uint8_t*)frame);
            } else {
                valueVector->reset();
                auto values = valueVector->getValues();
                uint64_t numElementsCopied = 0;
                while (numElementsCopied < size) {
                    node_offset_t currOffsetInPage = startOffset % NODE_SEQUENCE_VECTOR_SIZE;
                    auto pageIdx = getPageIdx(startOffset, numElementsPerPage);
                    auto frame = bufferManager.pin(fileHandle, pageIdx);
                    uint64_t numElementsToCopy = (numElementsPerPage - currOffsetInPage);
                    if (numElementsCopied + numElementsToCopy > size) {
                        numElementsToCopy = size - numElementsCopied;
                    }
                    memcpy((void*)(values + numElementsCopied * elementSize),
                        (void*)(frame +
                                getPageOffset(startOffset, numElementsPerPage, elementSize)),
                        numElementsToCopy * elementSize);
                    bufferManager.unpin(fileHandle, pageIdx);
                    numElementsCopied += numElementsToCopy;
                    startOffset += numElementsToCopy;
                }
            }
            return;
        }
        valueVector->reset();
        nodeID_t nodeID;
        auto values = valueVector->getValues();
        for (uint64_t i = 0; i < size; i++) {
            nodeIDVector->get(i, nodeID);
            auto pageIdx = getPageIdx(nodeID.offset, numElementsPerPage);
            auto frame = bufferManager.pin(fileHandle, pageIdx);
            memcpy((&values + i),
                (void*)(frame + getPageOffset(nodeID.offset, numElementsPerPage, elementSize)),
                elementSize);
            bufferManager.unpin(fileHandle, pageIdx);
        }
    }

    void reclaim(unique_ptr<VectorFrameHandle>& handle) {
        if (handle->isFrameBound) {
            bufferManager.unpin(fileHandle, handle->pageIdx);
        }
    }

    CompressionScheme getCompressionScheme() { return compressionScheme; }

    void setCompressionScheme(CompressionScheme compressionScheme) {
        this->compressionScheme = compressionScheme;
    }

    size_t getElementSize() { return elementSize; }

protected:
    size_t elementSize;
    uint32_t numElementsPerPage;
    // The compression scheme is only used for node ID values.
    CompressionScheme compressionScheme;
    FileHandle fileHandle;
    BufferManager& bufferManager;
};

} // namespace storage
} // namespace graphflow
