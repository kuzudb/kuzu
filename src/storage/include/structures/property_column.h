#pragma once

#include <string.h>

#include <iostream>

#include <bits/unique_ptr.h>

#include "src/common/include/vector/node_vector.h"
#include "src/common/include/vector/property_vector.h"
#include "src/storage/include/structures/column.h"

namespace graphflow {
namespace storage {

struct VectorFrameHandle {
    uint32_t pageIdx;
    bool isFrameBound;
    VectorFrameHandle() : isFrameBound(false), pageIdx(-1){};
};

template<typename T>
class PropertyColumn : public BaseColumn {

public:
    PropertyColumn(const string path, uint64_t numElements, BufferManager& bufferManager)
        : BaseColumn{path, sizeof(T), numElements, bufferManager} {};

    void readValues(const shared_ptr<BaseNodeIDVector>& nodeIDVector,
        const shared_ptr<PropertyVector<T>>& propertyVector, const uint64_t size,
        const unique_ptr<VectorFrameHandle>& handle) {
        if (nodeIDVector->isSequence()) {
            nodeID_t node;
            nodeIDVector->get(0, node);
            auto startOffset = node.offset;
            auto pageOffset = startOffset % numElementsPerPage;
            if (pageOffset + size <= numElementsPerPage) {
                handle->pageIdx = getPageIdx(startOffset, numElementsPerPage);
                handle->isFrameBound = true;
                auto frame = bufferManager.pin(fileHandle, handle->pageIdx);
                propertyVector->setValues((T*)frame);
            } else {
                propertyVector->reset();
                auto values = propertyVector->getValues();
                uint64_t numElementsCopied = 0;
                while (numElementsCopied < size) {
                    node_offset_t currOffsetInPage = startOffset % NODE_SEQUENCE_VECTOR_SIZE;
                    auto pageIdx = getPageIdx(startOffset, numElementsPerPage);
                    auto frame = bufferManager.pin(fileHandle, pageIdx);
                    uint64_t numElementsToCopy = (numElementsPerPage - currOffsetInPage);
                    if (numElementsCopied + numElementsToCopy > size) {
                        numElementsToCopy = size - numElementsCopied;
                    }
                    memcpy((void*)(values + numElementsCopied),
                        (void*)(frame + getPageOffset(startOffset, numElementsPerPage, sizeof(T))),
                        numElementsToCopy * sizeof(T));
                    bufferManager.unpin(fileHandle, pageIdx);
                    numElementsCopied += numElementsToCopy;
                    startOffset += numElementsToCopy;
                }
            }
            return;
        }
        propertyVector->reset();
        nodeID_t nodeID;
        T value;
        for (uint64_t i = 0; i < size; i++) {
            nodeIDVector->get(i, nodeID);
            auto pageIdx = getPageIdx(nodeID.offset, numElementsPerPage);
            auto frame = bufferManager.pin(fileHandle, pageIdx);
            memcpy(&value,
                (void*)(frame + getPageOffset(nodeID.offset, numElementsPerPage, sizeof(T))),
                sizeof(T));
            propertyVector->set(i, value);
            bufferManager.unpin(fileHandle, pageIdx);
        }
    }

    void reclaim(VectorFrameHandle* handle) {
        if (handle->isFrameBound) {
            bufferManager.unpin(fileHandle, handle->pageIdx);
        }
    }
};

typedef PropertyColumn<int32_t> PropertyColumnInt;
typedef PropertyColumn<double_t> PropertyColumnDouble;
typedef PropertyColumn<uint8_t> PropertyColumnBool;

} // namespace storage
} // namespace graphflow
