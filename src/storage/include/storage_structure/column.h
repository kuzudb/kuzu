#pragma once

#include "src/catalog/include/catalog.h"
#include "src/common/types/include/literal.h"
#include "src/storage/include/storage_structure/overflow_pages.h"
#include "src/storage/include/storage_structure/page_version_info.h"
#include "src/storage/include/storage_structure/storage_structure.h"
#include "src/storage/include/wal/wal.h"
#include "src/transaction/include/transaction.h"

using namespace graphflow::common;
using namespace graphflow::catalog;
using namespace std;

namespace graphflow {
namespace storage {

class Column : public StorageStructure {

public:
    Column(const string& fName, const Property property,
        const label_t nodeLabelForAdjColumnAndProperties, const size_t& elementSize,
        BufferManager& bufferManager, bool isInMemory, WAL* wal)
        : StorageStructure{fName, property.dataType, elementSize, bufferManager,
              true /*hasNULLBytes*/, isInMemory},
          property{property},
          nodeLabelForAdjColumnAndProperties{nodeLabelForAdjColumnAndProperties}, wal{wal},
          pageVersionInfo(fileHandle.getNumPages()){};

    Column(const string& fName, const Property property, const label_t nodeLabel,
        BufferManager& bufferManager, bool isInMemory, WAL* wal)
        : Column(fName, property, nodeLabel, Types::getDataTypeSize(property.dataType),
              bufferManager, isInMemory, wal){};

    virtual void readValues(
        const shared_ptr<ValueVector>& nodeIDVector, const shared_ptr<ValueVector>& valueVector) {
        transaction::Transaction tmpTransaction(transaction::READ_ONLY, UINT64_MAX);
        readValues(&tmpTransaction, nodeIDVector, valueVector);
    }

    virtual void readValues(const transaction::Transaction* transaction,
        const shared_ptr<ValueVector>& nodeIDVector, const shared_ptr<ValueVector>& valueVector);

    virtual void writeValueForFlatVector(const transaction::Transaction* transaction,
        const shared_ptr<ValueVector>& nodeIDVector, const shared_ptr<ValueVector>& resultVector);

    // Currently, used only in Loader tests.
    virtual Literal readValue(node_offset_t offset);
    // Used only for tests.
    bool isNull(node_offset_t nodeOffset);

protected:
    virtual void readForSingleNodeIDPosition(const transaction::Transaction* transaction,
        uint32_t pos, const shared_ptr<ValueVector>& nodeIDVector,
        const shared_ptr<ValueVector>& resultVector);

private:
    // Note: When storing edges of a single multiplicity edges in a column, this property does not
    // have a name or propertyID.
    catalog::Property property;
    // If this column is storing an adjacency column edges of rel properties of those edges, we also
    // store the src/dst nodeLabelForAdjColumnAndProperties of the property. This is needed to log
    // in the WAL the correct file ID of the column.
    // TODO(Semih); This is currently not used. It will be used when we support updates to
    // adj column edges and their rel properties.
    label_t nodeLabelForAdjColumnAndProperties;
    PageVersionInfo pageVersionInfo;
    WAL* wal;
};

class StringPropertyColumn : public Column {

public:
    StringPropertyColumn(const string& fName, const Property property,
        const label_t nodeLabelForAdjColumnAndProperties, BufferManager& bufferManager,
        bool isInMemory, WAL* wal)
        : Column{fName, property, nodeLabelForAdjColumnAndProperties, bufferManager, isInMemory,
              wal},
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
    ListPropertyColumn(const string& fName, const Property property,
        const label_t nodeLabelForAdjColumnAndProperties, BufferManager& bufferManager,
        bool isInMemory, WAL* wal)
        : Column{fName, property, nodeLabelForAdjColumnAndProperties, bufferManager, isInMemory,
              wal},
          listOverflowPages{fName, bufferManager, isInMemory} {};

    void readValues(const shared_ptr<ValueVector>& nodeIDVector,
        const shared_ptr<ValueVector>& valueVector) override;
    Literal readValue(node_offset_t offset) override;

private:
    OverflowPages listOverflowPages;
};

class AdjColumn : public Column {

public:
    AdjColumn(const string& fName, const label_t nodeLabelForAdjColumnAndProperties,
        const label_t relLabel, BufferManager& bufferManager,
        const NodeIDCompressionScheme& nodeIDCompressionScheme, bool isInMemory, WAL* wal)
        : Column{fName, Property::constructDummyPropertyForAdjColumnEdges(relLabel),
              nodeLabelForAdjColumnAndProperties, nodeIDCompressionScheme.getNumTotalBytes(),
              bufferManager, isInMemory, wal},
          nodeIDCompressionScheme(nodeIDCompressionScheme){};

private:
    void readForSingleNodeIDPosition(const transaction::Transaction* transaction, uint32_t pos,
        const shared_ptr<ValueVector>& nodeIDVector,
        const shared_ptr<ValueVector>& resultVector) override;

private:
    NodeIDCompressionScheme nodeIDCompressionScheme;
};

class ColumnFactory {

public:
    static unique_ptr<Column> getColumn(const string& fName, const Property property,
        const label_t nodeLabelForAdjColumnAndProperties, BufferManager& bufferManager,
        bool isInMemory, WAL* wal) {
        switch (property.dataType.typeID) {
        case INT64:
        case DOUBLE:
        case BOOL:
        case DATE:
        case TIMESTAMP:
        case INTERVAL:
            return make_unique<Column>(fName, property, nodeLabelForAdjColumnAndProperties,
                bufferManager, isInMemory, wal);
        case STRING:
            return make_unique<StringPropertyColumn>(fName, property,
                nodeLabelForAdjColumnAndProperties, bufferManager, isInMemory, wal);
        case LIST:
            return make_unique<ListPropertyColumn>(fName, property,
                nodeLabelForAdjColumnAndProperties, bufferManager, isInMemory, wal);
        default:
            throw StorageException("Invalid type for property column creation.");
        }
    }
};

} // namespace storage
} // namespace graphflow
