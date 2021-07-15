#pragma once

#include "src/common/include/gf_string.h"
#include "src/common/include/types.h"
#include "src/common/include/vector/node_vector.h"
#include "src/storage/include/data_structure/data_structure.h"
#include "src/storage/include/data_structure/string_overflow_pages.h"

using namespace graphflow::common;
using namespace std;

namespace graphflow {
namespace storage {

class BaseColumn : public DataStructure {

public:
    virtual void readValues(const shared_ptr<NodeIDVector>& nodeIDVector,
        const shared_ptr<ValueVector>& valueVector, const unique_ptr<PageHandle>& pageHandle,
        BufferManagerMetrics& metrics);

protected:
    BaseColumn(const string& fName, const DataType& dataType, const size_t& elementSize,
        const uint64_t& numElements, BufferManager& bufferManager, bool isInMemory)
        : DataStructure{fName, dataType, elementSize, bufferManager, isInMemory} {};

    void readFromNonSequentialLocations(const shared_ptr<NodeIDVector>& nodeIDVector,
        const shared_ptr<ValueVector>& valueVector, const unique_ptr<PageHandle>& pageHandle,
        BufferManagerMetrics& metrics);

public:
    static constexpr char COLUMN_SUFFIX[] = ".col";
};

template<DataType D>
class Column : public BaseColumn {

public:
    Column(const string& fName, const uint64_t& numElements, BufferManager& bufferManager,
        bool isInMemory)
        : BaseColumn{
              fName, D, TypeUtils::getDataTypeSize(D), numElements, bufferManager, isInMemory} {};
};

template<>
class Column<STRING> : public BaseColumn {

public:
    Column(const string& fName, const uint64_t& numElements, BufferManager& bufferManager,
        bool isInMemory)
        : BaseColumn{fName, STRING, sizeof(gf_string_t), numElements, bufferManager, isInMemory},
          stringOverflowPages{fName, bufferManager, isInMemory} {};

    void readValues(const shared_ptr<NodeIDVector>& nodeIDVector,
        const shared_ptr<ValueVector>& valueVector, const unique_ptr<PageHandle>& pageHandle,
        BufferManagerMetrics& metrics) override;

private:
    StringOverflowPages stringOverflowPages;
};

template<>
class Column<NODE> : public BaseColumn {

public:
    Column(const string& fName, const uint64_t& numElements, BufferManager& bufferManager,
        const NodeIDCompressionScheme& nodeIDCompressionScheme, bool isInMemory)
        : BaseColumn{fName, NODE, nodeIDCompressionScheme.getNumTotalBytes(), numElements,
              bufferManager, isInMemory},
          nodeIDCompressionScheme(nodeIDCompressionScheme){};

    NodeIDCompressionScheme getCompressionScheme() const { return nodeIDCompressionScheme; }

private:
    NodeIDCompressionScheme nodeIDCompressionScheme;
};

typedef Column<INT64> PropertyColumnInt64;
typedef Column<DOUBLE> PropertyColumnDouble;
typedef Column<BOOL> PropertyColumnBool;
typedef Column<STRING> PropertyColumnString;
typedef Column<NODE> AdjColumn;
typedef Column<DATE> PropertyColumnDate;

} // namespace storage
} // namespace graphflow
