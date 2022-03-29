#pragma once

#include "src/common/types/include/literal.h"
#include "src/storage/include/storage_structure/overflow_pages.h"
#include "src/storage/include/storage_structure/storage_structure.h"

using namespace graphflow::common;
using namespace std;

namespace graphflow {
namespace storage {

class Column : public StorageStructure {

public:
    Column(const string& fName, const DataType& dataType, const size_t& elementSize,
        const uint64_t& numElements, BufferManager& bufferManager, bool isInMemory)
        : StorageStructure{
              fName, dataType, elementSize, bufferManager, true /*hasNULLBytes*/, isInMemory} {};

    virtual void readValues(
        const shared_ptr<ValueVector>& nodeIDVector, const shared_ptr<ValueVector>& valueVector);

    // Currently, used only in Loader tests.
    virtual Literal readValue(node_offset_t offset);

protected:
    virtual void readForSingleNodeIDPosition(uint32_t pos,
        const shared_ptr<ValueVector>& nodeIDVector, const shared_ptr<ValueVector>& resultVector);
};

class StringPropertyColumn : public Column {

public:
    StringPropertyColumn(const string& fName, const uint64_t& numElements,
        BufferManager& bufferManager, bool isInMemory)
        : Column{fName, DataType(STRING), sizeof(gf_string_t), numElements, bufferManager,
              isInMemory},
          stringOverflowPages{fName, bufferManager, isInMemory} {};

    void readValues(const shared_ptr<ValueVector>& nodeIDVector,
        const shared_ptr<ValueVector>& valueVector) override;

    // Currently, used only in Loader tests.
    Literal readValue(node_offset_t offset) override;

private:
    OverflowPages stringOverflowPages;
};

class ListPropertyColumn : public Column {

public:
    ListPropertyColumn(const string& fName, const uint64_t& numElements,
        BufferManager& bufferManager, bool isInMemory, const DataType& dataType)
        : Column{fName, dataType, sizeof(gf_list_t), numElements, bufferManager, isInMemory},
          listOverflowPages{fName, bufferManager, isInMemory} {};

    void readValues(const shared_ptr<ValueVector>& nodeIDVector,
        const shared_ptr<ValueVector>& valueVector) override;
    Literal readValue(node_offset_t offset) override;

private:
    OverflowPages listOverflowPages;
};

class AdjColumn : public Column {

public:
    AdjColumn(const string& fName, const uint64_t& numElements, BufferManager& bufferManager,
        const NodeIDCompressionScheme& nodeIDCompressionScheme, bool isInMemory)
        : Column{fName, DataType(NODE), nodeIDCompressionScheme.getNumTotalBytes(), numElements,
              bufferManager, isInMemory},
          nodeIDCompressionScheme(nodeIDCompressionScheme){};

private:
    void readForSingleNodeIDPosition(uint32_t pos, const shared_ptr<ValueVector>& nodeIDVector,
        const shared_ptr<ValueVector>& resultVector) override;

private:
    NodeIDCompressionScheme nodeIDCompressionScheme;
};

class ColumnFactory {

public:
    static unique_ptr<Column> getColumn(const string& fName, const DataType& dataType,
        const uint64_t& numElements, BufferManager& bufferManager, bool isInMemory) {
        switch (dataType.typeID) {
        case INT64:
        case DOUBLE:
        case BOOL:
        case DATE:
        case TIMESTAMP:
        case INTERVAL:
            return make_unique<Column>(fName, dataType, Types::getDataTypeSize(dataType),
                numElements, bufferManager, isInMemory);
        case STRING:
            return make_unique<StringPropertyColumn>(fName, numElements, bufferManager, isInMemory);
        case LIST:
            return make_unique<ListPropertyColumn>(
                fName, numElements, bufferManager, isInMemory, dataType);
        default:
            throw invalid_argument("Invalid type for property column creation.");
        }
    }
};

} // namespace storage
} // namespace graphflow
