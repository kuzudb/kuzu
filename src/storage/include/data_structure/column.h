#pragma once

#include <string.h>

#include "src/common/include/string.h"
#include "src/common/include/types.h"
#include "src/common/include/vector/node_vector.h"
#include "src/storage/include/data_structure/data_structure.h"

using namespace graphflow::common;
using namespace std;

namespace graphflow {
namespace storage {

class BaseColumn : public DataStructure {

public:
    virtual ~BaseColumn() = default;

    virtual void readValues(const shared_ptr<NodeIDVector>& nodeIDVector,
        const shared_ptr<ValueVector>& valueVector, const unique_ptr<DataStructureHandle>& handle,
        BufferManagerMetrics& metrics);

protected:
    BaseColumn(const string& fname, const DataType& dataType, const size_t& elementSize,
        const uint64_t& numElements, BufferManager& bufferManager)
        : DataStructure{fname, dataType, elementSize, bufferManager} {};

    void readFromNonSequentialLocations(const shared_ptr<NodeIDVector>& nodeIDVector,
        const shared_ptr<ValueVector>& valueVector, const unique_ptr<DataStructureHandle>& handle,
        BufferManagerMetrics& metrics);
};

template<DataType D>
class Column : public BaseColumn {

public:
    Column(const string& path, const uint64_t& numElements, BufferManager& bufferManager)
        : BaseColumn{path, D, getDataTypeSize(D), numElements, bufferManager} {};
};

template<>
class Column<STRING> : public BaseColumn {

public:
    Column(const string& path, const uint64_t& numElements, BufferManager& bufferManager)
        : BaseColumn{path, STRING, sizeof(gf_string_t), numElements, bufferManager},
          overflowPagesFileHandle{path + ".ovf", O_RDWR} {};

    void readValues(const shared_ptr<NodeIDVector>& nodeIDVector,
        const shared_ptr<ValueVector>& valueVector, const unique_ptr<DataStructureHandle>& handle,
        BufferManagerMetrics& metrics) override;

private:
    void readStringsFromOverflowPages(
        const shared_ptr<ValueVector>& valueVector, BufferManagerMetrics& metrics);

private:
    FileHandle overflowPagesFileHandle;
};

template<>
class Column<NODE> : public BaseColumn {

public:
    Column(const string& path, const uint64_t& numElements, BufferManager& bufferManager,
        const NodeIDCompressionScheme& nodeIDCompressionScheme)
        : BaseColumn{path, NODE, nodeIDCompressionScheme.getNumTotalBytes(), numElements,
              bufferManager},
          nodeIDCompressionScheme(nodeIDCompressionScheme){};

    NodeIDCompressionScheme getCompressionScheme() const { return nodeIDCompressionScheme; }

private:
    NodeIDCompressionScheme nodeIDCompressionScheme;
};

typedef Column<INT32> PropertyColumnInt;
typedef Column<DOUBLE> PropertyColumnDouble;
typedef Column<BOOL> PropertyColumnBool;
typedef Column<STRING> PropertyColumnString;
typedef Column<NODE> AdjColumn;

} // namespace storage
} // namespace graphflow
