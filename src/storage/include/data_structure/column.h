#pragma once

#include "src/common/include/gf_string.h"
#include "src/common/include/literal.h"
#include "src/common/include/types.h"
#include "src/common/include/vector/node_vector.h"
#include "src/storage/include/data_structure/data_structure.h"
#include "src/storage/include/data_structure/string_overflow_pages.h"

using namespace graphflow::common;
using namespace std;

namespace graphflow {
namespace storage {

class Column : public DataStructure {

public:
    Column(const string& fName, const DataType& dataType, const size_t& elementSize,
        const uint64_t& numElements, BufferManager& bufferManager, bool isInMemory)
        : DataStructure{fName, dataType, elementSize, bufferManager, isInMemory} {};

    virtual void readValues(const shared_ptr<NodeIDVector>& nodeIDVector,
        const shared_ptr<ValueVector>& valueVector, BufferManagerMetrics& metrics);

    // Currently, used only in Loader tests.
    virtual Literal readValue(node_offset_t offset, BufferManagerMetrics& metrics);

protected:
    void readFromNonSequentialLocations(const shared_ptr<NodeIDVector>& nodeIDVector,
        const shared_ptr<ValueVector>& valueVector, BufferManagerMetrics& metrics);

public:
    static constexpr char COLUMN_SUFFIX[] = ".col";
};

class StringPropertyColumn : public Column {

public:
    StringPropertyColumn(const string& fName, const uint64_t& numElements,
        BufferManager& bufferManager, bool isInMemory)
        : Column{fName, STRING, sizeof(gf_string_t), numElements, bufferManager, isInMemory},
          stringOverflowPages{fName, bufferManager, isInMemory} {};

    void readValues(const shared_ptr<NodeIDVector>& nodeIDVector,
        const shared_ptr<ValueVector>& valueVector, BufferManagerMetrics& metrics) override;

    // Currently, used only in Loader tests.
    Literal readValue(node_offset_t offset, BufferManagerMetrics& metrics) override;

private:
    StringOverflowPages stringOverflowPages;
};

class AdjColumn : public Column {

public:
    AdjColumn(const string& fName, const uint64_t& numElements, BufferManager& bufferManager,
        const NodeIDCompressionScheme& nodeIDCompressionScheme, bool isInMemory)
        : Column{fName, NODE, nodeIDCompressionScheme.getNumTotalBytes(), numElements,
              bufferManager, isInMemory},
          nodeIDCompressionScheme(nodeIDCompressionScheme){};

    NodeIDCompressionScheme getCompressionScheme() const { return nodeIDCompressionScheme; }

private:
    NodeIDCompressionScheme nodeIDCompressionScheme;
};

class ColumnFactory {

public:
    static unique_ptr<Column> getColumn(const string& fName, const DataType& dataType,
        const uint64_t& numElements, BufferManager& bufferManager, bool isInMemory) {
        switch (dataType) {
        case INT64:
        case DOUBLE:
        case BOOL:
        case DATE:
            return make_unique<Column>(fName, dataType, TypeUtils::getDataTypeSize(dataType),
                numElements, bufferManager, isInMemory);
        case STRING:
            return make_unique<StringPropertyColumn>(fName, numElements, bufferManager, isInMemory);
        default:
            throw invalid_argument("Invalid type for property column creation.");
        }
    }
};

} // namespace storage
} // namespace graphflow
