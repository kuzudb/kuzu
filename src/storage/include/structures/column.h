#pragma once

#include <string.h>

#include <bits/unique_ptr.h>

#include "src/common/include/types.h"
#include "src/common/include/vector/node_vector.h"
#include "src/storage/include/structures/common.h"

using namespace graphflow::common;
using namespace std;

namespace graphflow {
namespace storage {

class BaseColumn : public BaseColumnOrList {

public:
    size_t getElementSize() { return elementSize; }

    virtual void readValues(const shared_ptr<NodeIDVector>& nodeIDVector,
        const shared_ptr<ValueVector>& valueVector, const uint64_t& size,
        const unique_ptr<VectorFrameHandle>& handle);

protected:
    BaseColumn(const string& fname, const size_t& elementSize, const uint64_t& numElements,
        BufferManager& bufferManager)
        : BaseColumnOrList{fname, elementSize, bufferManager} {};

    void readFromSeqNodeIDsBySettingFrame(const shared_ptr<ValueVector>& valueVector,
        const unique_ptr<VectorFrameHandle>& handle, node_offset_t startOffset);

    void readFromSeqNodeIDsByCopying(const shared_ptr<ValueVector>& valueVector,
        const uint64_t& size, const unique_ptr<VectorFrameHandle>& handle,
        node_offset_t startOffset);

    void readFromNonSeqNodeIDs(const shared_ptr<NodeIDVector>& nodeIDVector,
        const shared_ptr<ValueVector>& valueVector, const uint64_t& size,
        const unique_ptr<VectorFrameHandle>& handle);

private:
    inline uint64_t getPageIdx(const node_offset_t& nodeOffset) const {
        return nodeOffset / numElementsPerPage;
    }

    inline uint32_t getPageOffset(const node_offset_t& nodeOffset) const {
        return (nodeOffset % numElementsPerPage) * elementSize;
    }
};

template<typename T>
class Column : public BaseColumn {

public:
    Column(const string& path, const uint64_t& numElements, BufferManager& bufferManager)
        : BaseColumn{path, sizeof(T), numElements, bufferManager} {};
};

template<>
class Column<gf_string_t> : public BaseColumn {

public:
    Column(const string& path, const uint64_t& numElements, BufferManager& bufferManager)
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
    Column(const string& path, const uint64_t& numElements, BufferManager& bufferManager,
        const NodeIDCompressionScheme& nodeIDCompressionScheme)
        : BaseColumn{path, nodeIDCompressionScheme.getNumTotalBytes(), numElements, bufferManager},
          nodeIDCompressionScheme(nodeIDCompressionScheme){};

    NodeIDCompressionScheme getCompressionScheme() const { return nodeIDCompressionScheme; }

private:
    NodeIDCompressionScheme nodeIDCompressionScheme;
};

typedef Column<int32_t> PropertyColumnInt;
typedef Column<double_t> PropertyColumnDouble;
typedef Column<uint8_t> PropertyColumnBool;
typedef Column<gf_string_t> PropertyColumnString;
typedef Column<nodeID_t> AdjColumn;

} // namespace storage
} // namespace graphflow
