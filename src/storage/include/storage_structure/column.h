#pragma once

#include "src/catalog/include/catalog.h"
#include "src/common/types/include/literal.h"
#include "src/storage/include/storage_structure/overflow_pages.h"
#include "src/storage/include/storage_structure/storage_structure.h"

using namespace graphflow::common;
using namespace graphflow::catalog;
using namespace std;

namespace graphflow {
namespace storage {

class Column : public BaseColumnOrList {

public:
    Column(const StorageStructureIDAndFName structureIDAndFName, const DataType& dataType,
        size_t elementSize, BufferManager& bufferManager, bool isInMemory, WAL* wal)
        : BaseColumnOrList{structureIDAndFName, dataType, elementSize, bufferManager,
              true /*hasNULLBytes*/, isInMemory, wal} {};

    Column(const StorageStructureIDAndFName structureIDAndFName, const DataType& dataType,
        BufferManager& bufferManager, bool isInMemory, WAL* wal)
        : Column(structureIDAndFName, dataType, Types::getDataTypeSize(dataType), bufferManager,
              isInMemory, wal){};

    virtual void readValues(Transaction* transaction, const shared_ptr<ValueVector>& nodeIDVector,
        const shared_ptr<ValueVector>& valueVector);

    virtual void writeValues(const shared_ptr<ValueVector>& nodeIDVector,
        const shared_ptr<ValueVector>& vectorToWriteFrom);

    // Currently, used only in Loader tests.
    virtual Literal readValue(node_offset_t offset);
    // Used only for tests.
    bool isNull(node_offset_t nodeOffset);

protected:
    // If necessary creates a second version (backed by the WAL) of a page that contains the fixed
    // length part of the value that will be written to.
    // Obtains *and does not release* the lock original page. Pins and updates the WAL version of
    // the page. Finally updates the page with the new value from vectorToWriteFrom.
    // Note that caller must ensure to unpin and release the WAL version of the page by calling
    // StorageStructure::finishUpdatingPage.
    UpdatedPageInfoAndWALPageFrame beginUpdatingPage(node_offset_t nodeOffset,
        const shared_ptr<ValueVector>& vectorToWriteFrom, uint32_t posInVectorToWriteFrom);
    virtual void writeValueForSingleNodeIDPosition(node_offset_t nodeOffset,
        const shared_ptr<ValueVector>& vectorToWriteFrom, uint32_t posInVectorToWriteFrom);

    virtual inline void readSequential(Transaction* transaction,
        const shared_ptr<ValueVector>& resultVector, uint64_t sizeToRead, PageElementCursor& cursor,
        const std::function<uint32_t(uint32_t)>& logicalToPhysicalPageMapper) {
        readBySequentialCopy(
            transaction, resultVector, sizeToRead, cursor, logicalToPhysicalPageMapper);
    }

    virtual void readForSingleNodeIDPosition(Transaction* transaction, uint32_t pos,
        const shared_ptr<ValueVector>& nodeIDVector, const shared_ptr<ValueVector>& resultVector);
};

class StringPropertyColumn : public Column {

public:
    StringPropertyColumn(const StorageStructureIDAndFName structureIDAndFNameOfMainColumn,
        const DataType& dataType, BufferManager& bufferManager, bool isInMemory, WAL* wal)
        : Column{structureIDAndFNameOfMainColumn, dataType, bufferManager, isInMemory, wal},
          stringOverflowPages{structureIDAndFNameOfMainColumn, bufferManager, isInMemory, wal} {};

    void readValues(Transaction* transaction, const shared_ptr<ValueVector>& nodeIDVector,
        const shared_ptr<ValueVector>& valueVector) override;

    void writeValueForSingleNodeIDPosition(node_offset_t nodeOffset,
        const shared_ptr<ValueVector>& vectorToWriteFrom, uint32_t posInVectorToWriteFrom);

    // Currently, used only in Loader tests.
    Literal readValue(node_offset_t offset) override;

    inline VersionedFileHandle* getOverflowFileHandle() {
        return stringOverflowPages.getFileHandle();
    }

private:
    OverflowPages stringOverflowPages;
};

class ListPropertyColumn : public Column {

public:
    ListPropertyColumn(const StorageStructureIDAndFName structureIDAndFNameOfMainColumn,
        const DataType& dataType, BufferManager& bufferManager, bool isInMemory, WAL* wal)
        : Column{structureIDAndFNameOfMainColumn, dataType, bufferManager, isInMemory, wal},
          listOverflowPages{structureIDAndFNameOfMainColumn, bufferManager, isInMemory, wal} {};

    void readValues(Transaction* transaction, const shared_ptr<ValueVector>& nodeIDVector,
        const shared_ptr<ValueVector>& valueVector) override;
    Literal readValue(node_offset_t offset) override;

private:
    OverflowPages listOverflowPages;
};

class AdjColumn : public Column {

public:
    AdjColumn(const StorageStructureIDAndFName structureIDAndFName, BufferManager& bufferManager,
        const NodeIDCompressionScheme& nodeIDCompressionScheme, bool isInMemory, WAL* wal)
        : Column{structureIDAndFName, DataType(NODE_ID), nodeIDCompressionScheme.getNumTotalBytes(),
              bufferManager, isInMemory, wal},
          nodeIDCompressionScheme(nodeIDCompressionScheme){};

private:
    inline void readSequential(Transaction* transaction,
        const shared_ptr<ValueVector>& resultVector, uint64_t sizeToRead, PageElementCursor& cursor,
        const std::function<uint32_t(uint32_t)>& logicalToPhysicalPageMapper) override {
        readNodeIDsFromSequentialPages(resultVector, cursor, logicalToPhysicalPageMapper,
            nodeIDCompressionScheme, false /*isAdjLists*/);
    }

    void readForSingleNodeIDPosition(Transaction* transaction, uint32_t pos,
        const shared_ptr<ValueVector>& nodeIDVector,
        const shared_ptr<ValueVector>& resultVector) override;

private:
    NodeIDCompressionScheme nodeIDCompressionScheme;
};

class ColumnFactory {

public:
    static unique_ptr<Column> getColumn(const StorageStructureIDAndFName structureIDAndFName,
        const DataType& dataType, BufferManager& bufferManager, bool isInMemory, WAL* wal) {
        switch (dataType.typeID) {
        case INT64:
        case DOUBLE:
        case BOOL:
        case DATE:
        case TIMESTAMP:
        case INTERVAL:
            return make_unique<Column>(
                structureIDAndFName, dataType, bufferManager, isInMemory, wal);
        case STRING:
            return make_unique<StringPropertyColumn>(
                structureIDAndFName, dataType, bufferManager, isInMemory, wal);
        case LIST:
            return make_unique<ListPropertyColumn>(
                structureIDAndFName, dataType, bufferManager, isInMemory, wal);
        default:
            throw StorageException("Invalid type for property column creation.");
        }
    }
};

} // namespace storage
} // namespace graphflow
