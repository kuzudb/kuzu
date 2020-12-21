#pragma once

#include <string.h>

#include <bits/unique_ptr.h>

#include "src/common/include/configs.h"
#include "src/common/include/types.h"
#include "src/common/include/vector/node_vector.h"
#include "src/storage/include/buffer_manager.h"
#include "src/storage/include/file_handle.h"

using namespace graphflow::common;
using namespace std;

namespace graphflow {
namespace storage {

struct VectorFrameHandle {
    uint32_t pageIdx;
    bool isFrameBound;
    VectorFrameHandle() : pageIdx(-1), isFrameBound(false){};
};

class BaseColumn {

public:
    BaseColumn(
        const string fname, size_t elementSize, uint64_t numElements, BufferManager& bufferManager)
        : elementSize(elementSize), numElementsPerPage{(uint32_t)(PAGE_SIZE / elementSize)},
          fileHandle{fname, 1 + (numElements / numElementsPerPage)}, bufferManager{bufferManager} {}

    inline static uint64_t getPageIdx(node_offset_t nodeOffset, uint32_t numElementsPerPage) {
        return nodeOffset / numElementsPerPage;
    }

    inline static uint32_t getPageOffset(
        node_offset_t nodeOffset, uint32_t numElementsPerPage, size_t elementSize) {
        return (nodeOffset % numElementsPerPage) * elementSize;
    }

    size_t getElementSize() { return elementSize; }

    virtual void readValues(const shared_ptr<NodeIDVector>& nodeIDVector,
        const shared_ptr<ValueVector>& valueVector, const uint64_t size,
        const unique_ptr<VectorFrameHandle>& handle) {
        if (nodeIDVector->getIsSequence()) {
            nodeID_t node;
            nodeIDVector->readValue(0, node);
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
                    node_offset_t currOffsetInPage = startOffset % numElementsPerPage;
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
            nodeIDVector->readValue(i, nodeID);
            auto pageIdx = getPageIdx(nodeID.offset, numElementsPerPage);
            auto frame = bufferManager.pin(fileHandle, pageIdx);
            memcpy((void*)(values + i * elementSize),
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

protected:
    size_t elementSize;
    uint32_t numElementsPerPage;
    FileHandle fileHandle;
    BufferManager& bufferManager;
};

template<typename T>
class Column : public BaseColumn {

public:
    Column(const string path, uint64_t numElements, BufferManager& bufferManager)
        : BaseColumn{path, sizeof(T), numElements, bufferManager} {};
};

template<>
class Column<nodeID_t> : public BaseColumn {

public:
    Column(const string path, size_t elementSize, uint64_t numElements,
        BufferManager& bufferManager, NodeIDCompressionScheme compressionScheme)
        : BaseColumn{path, elementSize, numElements, bufferManager} {};

    NodeIDCompressionScheme getCompressionScheme() { return compressionScheme; }

private:
    NodeIDCompressionScheme compressionScheme;
};

typedef Column<int32_t> PropertyColumnInt;
typedef Column<double_t> PropertyColumnDouble;
typedef Column<uint8_t> PropertyColumnBool;
typedef Column<nodeID_t> AdjColumn;

} // namespace storage
} // namespace graphflow
