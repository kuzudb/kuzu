#pragma once

#include <string.h>

#include "src/common/include/types.h"
#include "src/common/include/vector/node_vector.h"
#include "src/storage/include/structures/common.h"

using namespace graphflow::common;
using namespace std;

namespace graphflow {
namespace storage {

class BaseColumn : public BaseColumnOrLists {

public:
    virtual ~BaseColumn() = default;

    virtual void readValues(const shared_ptr<NodeIDVector>& nodeIDVector,
        const shared_ptr<ValueVector>& valueVector, uint64_t size,
        const unique_ptr<ColumnOrListsHandle>& handle);

protected:
    BaseColumn(const string& fname, const size_t& elementSize, const uint64_t& numElements,
        BufferManager& bufferManager)
        : BaseColumnOrLists{fname, elementSize, bufferManager} {};

    void readFromNonSequentialLocations(const shared_ptr<NodeIDVector>& nodeIDVector,
        const shared_ptr<ValueVector>& valueVector, uint64_t size,
        const unique_ptr<ColumnOrListsHandle>& handle);
};

template<typename T>
class Column : public BaseColumn {

public:
    Column(const string& path, const uint64_t& numElements, BufferManager& bufferManager)
        : BaseColumn{path, sizeof(T), numElements, bufferManager} {};

    DataType getDataType() override;
};

template<>
class Column<gf_string_t> : public BaseColumn {

public:
    Column(const string& path, const uint64_t& numElements, BufferManager& bufferManager)
        : BaseColumn{path, sizeof(gf_string_t), numElements, bufferManager},
          overflowPagesFileHandle{path + ".ovf"} {};

    void readValues(const shared_ptr<NodeIDVector>& nodeIDVector,
        const shared_ptr<ValueVector>& valueVector, uint64_t size,
        const unique_ptr<ColumnOrListsHandle>& handle) override;

    DataType getDataType() override { return STRING; }

private:
    void readStringsFromOverflowPages(const shared_ptr<ValueVector>& valueVector, uint64_t size);

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

    DataType getDataType() override { return NODE; }

private:
    NodeIDCompressionScheme nodeIDCompressionScheme;
};

template<>
DataType Column<int32_t>::getDataType();

template<>
DataType Column<double_t>::getDataType();

template<>
DataType Column<uint8_t>::getDataType();

typedef Column<int32_t> PropertyColumnInt;
typedef Column<double_t> PropertyColumnDouble;
typedef Column<uint8_t> PropertyColumnBool;
typedef Column<gf_string_t> PropertyColumnString;
typedef Column<nodeID_t> AdjColumn;

} // namespace storage
} // namespace graphflow
