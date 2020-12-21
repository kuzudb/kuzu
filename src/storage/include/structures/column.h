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
          propertyColumnFileHandle{fname}, bufferManager{bufferManager} {}

    inline uint64_t getPageIdx(node_offset_t nodeOffset, uint32_t numElementsPerPage) {
        return nodeOffset / numElementsPerPage;
    }

    inline uint32_t getPageOffset(
        node_offset_t nodeOffset, uint32_t numElementsPerPage, size_t elementSize) {
        return (nodeOffset % numElementsPerPage) * elementSize;
    }

    size_t getElementSize() { return elementSize; }

    virtual void readValues(const shared_ptr<NodeIDVector>& nodeIDVector,
        const shared_ptr<ValueVector>& valueVector, const uint64_t& size,
        const unique_ptr<VectorFrameHandle>& handle);

    void reclaim(unique_ptr<VectorFrameHandle>& handle);

protected:
    void readFromSeqNodeIDsBySettingFrame(const shared_ptr<ValueVector>& valueVector,
        const unique_ptr<VectorFrameHandle>& handle, node_offset_t startOffset);

    void readFromSeqNodeIDsByCopying(const shared_ptr<ValueVector>& valueVector,
        const uint64_t& size, node_offset_t startOffset);

    void readFromNonSeqNodeIDs(const shared_ptr<NodeIDVector>& nodeIDVector,
        const shared_ptr<ValueVector>& valueVector, const uint64_t& size);

protected:
    size_t elementSize;
    uint32_t numElementsPerPage;
    FileHandle propertyColumnFileHandle;
    BufferManager& bufferManager;
};

template<typename T>
class Column : public BaseColumn {

public:
    Column(const string path, uint64_t numElements, BufferManager& bufferManager)
        : BaseColumn{path, sizeof(T), numElements, bufferManager} {};
};

template<>
class Column<gf_string_t> : public BaseColumn {

public:
    Column(const string path, uint64_t numElements, BufferManager& bufferManager)
        : BaseColumn{path, sizeof(gf_string_t), numElements, bufferManager},
          overflowPagesFileHandle{path + ".ovf"} {};

    void readValues(const shared_ptr<NodeIDVector>& nodeIDVector,
        const shared_ptr<ValueVector>& valueVector, const uint64_t& size,
        const unique_ptr<VectorFrameHandle>& handle) override;

private:
    void readStringsFromOverflowPages(
        const shared_ptr<ValueVector>& valueVector, const uint64_t& size);

private:
    FileHandle overflowPagesFileHandle;
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
typedef Column<gf_string_t> PropertyColumnString;
typedef Column<nodeID_t> AdjColumn;

} // namespace storage
} // namespace graphflow
