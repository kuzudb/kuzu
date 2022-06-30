#pragma once

#include "src/catalog/include/catalog.h"
#include "src/common/types/include/literal.h"
#include "src/storage/storage_structure/include/overflow_file.h"
#include "src/storage/storage_structure/include/storage_structure.h"

using namespace graphflow::common;
using namespace graphflow::catalog;
using namespace std;

namespace graphflow {
namespace storage {

class Column : public BaseColumnOrList {

public:
    Column(const StorageStructureIDAndFName& structureIDAndFName, const DataType& dataType,
        size_t elementSize, BufferManager& bufferManager, bool isInMemory, WAL* wal)
        : BaseColumnOrList{structureIDAndFName, dataType, elementSize, bufferManager,
              true /*hasNULLBytes*/, isInMemory, wal} {};

    Column(const StorageStructureIDAndFName& structureIDAndFName, const DataType& dataType,
        BufferManager& bufferManager, bool isInMemory, WAL* wal)
        : Column(structureIDAndFName, dataType, Types::getDataTypeSize(dataType), bufferManager,
              isInMemory, wal){};

    void read(Transaction* transaction, const shared_ptr<ValueVector>& nodeIDVector,
        const shared_ptr<ValueVector>& resultVector);

    void writeValues(const shared_ptr<ValueVector>& nodeIDVector,
        const shared_ptr<ValueVector>& vectorToWriteFrom);

    // Currently, used only in Loader tests.
    virtual Literal readValue(node_offset_t offset);
    // Used only for tests.
    bool isNull(node_offset_t nodeOffset);

protected:
    void lookup(Transaction* transaction, const shared_ptr<ValueVector>& nodeIDVector,
        const shared_ptr<ValueVector>& resultVector, uint32_t vectorPos);

    virtual void lookup(Transaction* transaction, const shared_ptr<ValueVector>& resultVector,
        uint32_t vectorPos, PageCursor& cursor);
    virtual inline void scan(
        Transaction* transaction, const shared_ptr<ValueVector>& resultVector, PageCursor& cursor) {
        readBySequentialCopy(transaction, resultVector, cursor, identityMapper);
    }
    virtual void scanWithSelState(
        Transaction* transaction, const shared_ptr<ValueVector>& resultVector, PageCursor& cursor) {
        readBySequentialCopyWithSelState(transaction, resultVector, cursor, identityMapper);
    }

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

protected:
    // no logical-physical page mapping is required for columns
    std::function<page_idx_t(page_idx_t)> identityMapper = [](uint32_t i) { return i; };
};

class StringPropertyColumn : public Column {

public:
    StringPropertyColumn(const StorageStructureIDAndFName& structureIDAndFNameOfMainColumn,
        const DataType& dataType, BufferManager& bufferManager, bool isInMemory, WAL* wal)
        : Column{structureIDAndFNameOfMainColumn, dataType, bufferManager, isInMemory, wal},
          stringOverflowPages{structureIDAndFNameOfMainColumn, bufferManager, isInMemory, wal} {};

    void writeValueForSingleNodeIDPosition(node_offset_t nodeOffset,
        const shared_ptr<ValueVector>& vectorToWriteFrom, uint32_t posInVectorToWriteFrom) override;

    // Currently, used only in Loader tests.
    Literal readValue(node_offset_t offset) override;

    inline VersionedFileHandle* getOverflowFileHandle() {
        return stringOverflowPages.getFileHandle();
    }

private:
    inline void lookup(Transaction* transaction, const shared_ptr<ValueVector>& resultVector,
        uint32_t vectorPos, PageCursor& cursor) override {
        Column::lookup(transaction, resultVector, vectorPos, cursor);
        if (!resultVector->isNull(vectorPos)) {
            stringOverflowPages.scanSingleStringOverflow(transaction, *resultVector, vectorPos);
        }
    }
    inline void scan(Transaction* transaction, const shared_ptr<ValueVector>& resultVector,
        PageCursor& cursor) override {
        Column::scan(transaction, resultVector, cursor);
        stringOverflowPages.scanSequentialStringOverflow(transaction, *resultVector);
    }
    void scanWithSelState(Transaction* transaction, const shared_ptr<ValueVector>& resultVector,
        PageCursor& cursor) override {
        Column::scanWithSelState(transaction, resultVector, cursor);
        stringOverflowPages.scanSequentialStringOverflow(transaction, *resultVector);
    }

private:
    OverflowFile stringOverflowPages;
};

class ListPropertyColumn : public Column {

public:
    ListPropertyColumn(const StorageStructureIDAndFName& structureIDAndFNameOfMainColumn,
        const DataType& dataType, BufferManager& bufferManager, bool isInMemory, WAL* wal)
        : Column{structureIDAndFNameOfMainColumn, dataType, bufferManager, isInMemory, wal},
          listOverflowPages{structureIDAndFNameOfMainColumn, bufferManager, isInMemory, wal} {};

    void writeValueForSingleNodeIDPosition(node_offset_t nodeOffset,
        const shared_ptr<ValueVector>& vectorToWriteFrom, uint32_t posInVectorToWriteFrom) override;

    Literal readValue(node_offset_t offset) override;

private:
    inline void lookup(Transaction* transaction, const shared_ptr<ValueVector>& resultVector,
        uint32_t vectorPos, PageCursor& cursor) override {
        Column::lookup(transaction, resultVector, vectorPos, cursor);
        listOverflowPages.readListsToVector(*resultVector);
    }
    inline void scan(Transaction* transaction, const shared_ptr<ValueVector>& resultVector,
        PageCursor& cursor) override {
        Column::scan(transaction, resultVector, cursor);
        listOverflowPages.readListsToVector(*resultVector);
    }
    inline void scanWithSelState(Transaction* transaction,
        const shared_ptr<ValueVector>& resultVector, PageCursor& cursor) override {
        Column::scanWithSelState(transaction, resultVector, cursor);
        listOverflowPages.readListsToVector(*resultVector);
    }

private:
    OverflowFile listOverflowPages;
};

class AdjColumn : public Column {

public:
    AdjColumn(const StorageStructureIDAndFName& structureIDAndFName, BufferManager& bufferManager,
        const NodeIDCompressionScheme& nodeIDCompressionScheme, bool isInMemory, WAL* wal)
        : Column{structureIDAndFName, DataType(NODE_ID), nodeIDCompressionScheme.getNumTotalBytes(),
              bufferManager, isInMemory, wal},
          nodeIDCompressionScheme(nodeIDCompressionScheme){};

private:
    inline void lookup(Transaction* transaction, const shared_ptr<ValueVector>& resultVector,
        uint32_t vectorPos, PageCursor& cursor) override {
        readNodeIDsFromAPageBySequentialCopy(resultVector, vectorPos, cursor.pageIdx,
            cursor.posInPage, 1 /* numValuesToCopy */, nodeIDCompressionScheme,
            false /*isAdjLists*/);
    }
    inline void scan(Transaction* transaction, const shared_ptr<ValueVector>& resultVector,
        PageCursor& cursor) override {
        readNodeIDsBySequentialCopy(
            resultVector, cursor, identityMapper, nodeIDCompressionScheme, false /*isAdjLists*/);
    }
    inline void scanWithSelState(Transaction* transaction,
        const shared_ptr<ValueVector>& resultVector, PageCursor& cursor) override {
        readNodeIDsBySequentialCopyWithSelState(
            resultVector, cursor, identityMapper, nodeIDCompressionScheme);
    }

private:
    NodeIDCompressionScheme nodeIDCompressionScheme;
};

class ColumnFactory {

public:
    static unique_ptr<Column> getColumn(const StorageStructureIDAndFName& structureIDAndFName,
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
