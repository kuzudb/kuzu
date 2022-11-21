#pragma once

#include "catalog/catalog.h"
#include "common/types/literal.h"
#include "storage/storage_structure/disk_overflow_file.h"
#include "storage/storage_structure/storage_structure.h"

using namespace kuzu::common;
using namespace kuzu::catalog;
using namespace std;

namespace kuzu {
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

    // Currently, used only in CopyCSV tests.
    virtual Literal readValue(node_offset_t offset);
    bool isNull(node_offset_t nodeOffset, Transaction* transaction);
    void setNodeOffsetToNull(node_offset_t nodeOffset);

protected:
    void lookup(Transaction* transaction, const shared_ptr<ValueVector>& nodeIDVector,
        const shared_ptr<ValueVector>& resultVector, uint32_t vectorPos);

    virtual void lookup(Transaction* transaction, const shared_ptr<ValueVector>& resultVector,
        uint32_t vectorPos, PageElementCursor& cursor);
    virtual inline void scan(Transaction* transaction, const shared_ptr<ValueVector>& resultVector,
        PageElementCursor& cursor) {
        readBySequentialCopy(transaction, resultVector, cursor, identityMapper);
    }
    virtual void scanWithSelState(Transaction* transaction,
        const shared_ptr<ValueVector>& resultVector, PageElementCursor& cursor) {
        readBySequentialCopyWithSelState(transaction, resultVector, cursor, identityMapper);
    }
    virtual void writeValueForSingleNodeIDPosition(node_offset_t nodeOffset,
        const shared_ptr<ValueVector>& vectorToWriteFrom, uint32_t posInVectorToWriteFrom);
    WALPageIdxPosInPageAndFrame beginUpdatingPage(node_offset_t nodeOffset,
        const shared_ptr<ValueVector>& vectorToWriteFrom, uint32_t posInVectorToWriteFrom);

private:
    // The reason why we make this function virtual is: we can't simply do memcpy on nodeIDs if
    // the adjColumn has tableIDCompression, in this case we only store the nodeOffset in
    // persistent store of adjColumn.
    virtual inline void writeToPage(WALPageIdxPosInPageAndFrame& walPageInfo,
        const shared_ptr<ValueVector>& vectorToWriteFrom, uint32_t posInVectorToWriteFrom) {
        memcpy(walPageInfo.frame + mapElementPosToByteOffset(walPageInfo.posInPage),
            vectorToWriteFrom->getData() + posInVectorToWriteFrom * elementSize, elementSize);
    }
    // If necessary creates a second version (backed by the WAL) of a page that contains the fixed
    // length part of the value that will be written to.
    // Obtains *and does not release* the lock original page. Pins and updates the WAL version of
    // the page. Finally updates the page with the new value from vectorToWriteFrom.
    // Note that caller must ensure to unpin and release the WAL version of the page by calling
    // StorageStructure::unpinWALPageAndReleaseOriginalPageLock.
    WALPageIdxPosInPageAndFrame beginUpdatingPageAndWriteOnlyNullBit(
        node_offset_t nodeOffset, bool isNull);

protected:
    // no logical-physical page mapping is required for columns
    std::function<page_idx_t(page_idx_t)> identityMapper = [](uint32_t i) { return i; };
};

class PropertyColumnWithOverflow : public Column {
public:
    PropertyColumnWithOverflow(const StorageStructureIDAndFName& structureIDAndFNameOfMainColumn,
        const DataType& dataType, BufferManager& bufferManager, bool isInMemory, WAL* wal)
        : Column{structureIDAndFNameOfMainColumn, dataType, bufferManager, isInMemory, wal},
          diskOverflowFile{structureIDAndFNameOfMainColumn, bufferManager, isInMemory, wal} {}

    inline DiskOverflowFile* getDiskOverflowFile() { return &diskOverflowFile; }

    inline VersionedFileHandle* getDiskOverflowFileHandle() {
        return diskOverflowFile.getFileHandle();
    }

protected:
    DiskOverflowFile diskOverflowFile;
};

class StringPropertyColumn : public PropertyColumnWithOverflow {

public:
    StringPropertyColumn(const StorageStructureIDAndFName& structureIDAndFNameOfMainColumn,
        const DataType& dataType, BufferManager& bufferManager, bool isInMemory, WAL* wal)
        : PropertyColumnWithOverflow{
              structureIDAndFNameOfMainColumn, dataType, bufferManager, isInMemory, wal} {};

    void writeValueForSingleNodeIDPosition(node_offset_t nodeOffset,
        const shared_ptr<ValueVector>& vectorToWriteFrom, uint32_t posInVectorToWriteFrom) override;

    // Currently, used only in CopyCSV tests.
    Literal readValue(node_offset_t offset) override;

private:
    inline void lookup(Transaction* transaction, const shared_ptr<ValueVector>& resultVector,
        uint32_t vectorPos, PageElementCursor& cursor) override {
        Column::lookup(transaction, resultVector, vectorPos, cursor);
        if (!resultVector->isNull(vectorPos)) {
            diskOverflowFile.scanSingleStringOverflow(
                transaction->getType(), *resultVector, vectorPos);
        }
    }
    inline void scan(Transaction* transaction, const shared_ptr<ValueVector>& resultVector,
        PageElementCursor& cursor) override {
        Column::scan(transaction, resultVector, cursor);
        diskOverflowFile.scanSequentialStringOverflow(transaction->getType(), *resultVector);
    }
    void scanWithSelState(Transaction* transaction, const shared_ptr<ValueVector>& resultVector,
        PageElementCursor& cursor) override {
        Column::scanWithSelState(transaction, resultVector, cursor);
        diskOverflowFile.scanSequentialStringOverflow(transaction->getType(), *resultVector);
    }
};

class ListPropertyColumn : public PropertyColumnWithOverflow {

public:
    ListPropertyColumn(const StorageStructureIDAndFName& structureIDAndFNameOfMainColumn,
        const DataType& dataType, BufferManager& bufferManager, bool isInMemory, WAL* wal)
        : PropertyColumnWithOverflow{
              structureIDAndFNameOfMainColumn, dataType, bufferManager, isInMemory, wal} {};

    void writeValueForSingleNodeIDPosition(node_offset_t nodeOffset,
        const shared_ptr<ValueVector>& vectorToWriteFrom, uint32_t posInVectorToWriteFrom) override;

    Literal readValue(node_offset_t offset) override;

private:
    inline void lookup(Transaction* transaction, const shared_ptr<ValueVector>& resultVector,
        uint32_t vectorPos, PageElementCursor& cursor) override {
        Column::lookup(transaction, resultVector, vectorPos, cursor);
        if (!resultVector->isNull(vectorPos)) {
            diskOverflowFile.scanSingleListOverflow(
                transaction->getType(), *resultVector, vectorPos);
        }
    }
    inline void scan(Transaction* transaction, const shared_ptr<ValueVector>& resultVector,
        PageElementCursor& cursor) override {
        Column::scan(transaction, resultVector, cursor);
        diskOverflowFile.readListsToVector(transaction->getType(), *resultVector);
    }
    inline void scanWithSelState(Transaction* transaction,
        const shared_ptr<ValueVector>& resultVector, PageElementCursor& cursor) override {
        Column::scanWithSelState(transaction, resultVector, cursor);
        diskOverflowFile.readListsToVector(transaction->getType(), *resultVector);
    }
};

class AdjColumn : public Column {

public:
    AdjColumn(const StorageStructureIDAndFName& structureIDAndFName, BufferManager& bufferManager,
        const NodeIDCompressionScheme& nodeIDCompressionScheme, bool isInMemory, WAL* wal)
        : Column{structureIDAndFName, DataType(NODE_ID),
              nodeIDCompressionScheme.getNumBytesForNodeIDAfterCompression(), bufferManager,
              isInMemory, wal},
          nodeIDCompressionScheme(nodeIDCompressionScheme){};

private:
    inline void lookup(Transaction* transaction, const shared_ptr<ValueVector>& resultVector,
        uint32_t vectorPos, PageElementCursor& cursor) override {
        readNodeIDsFromAPageBySequentialCopy(transaction, resultVector, vectorPos, cursor.pageIdx,
            cursor.elemPosInPage, 1 /* numValuesToCopy */, nodeIDCompressionScheme,
            false /*isAdjLists*/);
    }
    inline void scan(Transaction* transaction, const shared_ptr<ValueVector>& resultVector,
        PageElementCursor& cursor) override {
        readNodeIDsBySequentialCopy(transaction, resultVector, cursor, identityMapper,
            nodeIDCompressionScheme, false /*isAdjLists*/);
    }
    inline void scanWithSelState(Transaction* transaction,
        const shared_ptr<ValueVector>& resultVector, PageElementCursor& cursor) override {
        readNodeIDsBySequentialCopyWithSelState(
            transaction, resultVector, cursor, identityMapper, nodeIDCompressionScheme);
    }
    inline void writeToPage(WALPageIdxPosInPageAndFrame& walPageInfo,
        const shared_ptr<ValueVector>& vectorToWriteFrom,
        uint32_t posInVectorToWriteFrom) override {
        nodeIDCompressionScheme.writeNodeID(
            walPageInfo.frame + mapElementPosToByteOffset(walPageInfo.posInPage),
            vectorToWriteFrom->getValue<nodeID_t>(posInVectorToWriteFrom));
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
} // namespace kuzu
