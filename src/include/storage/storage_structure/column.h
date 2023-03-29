#pragma once

#include "catalog/catalog.h"
#include "common/types/value.h"
#include "storage/storage_structure/disk_overflow_file.h"
#include "storage/storage_structure/storage_structure.h"

namespace kuzu {
namespace storage {

class Column : public BaseColumnOrList {

public:
    Column(const StorageStructureIDAndFName& structureIDAndFName, const common::DataType& dataType,
        size_t elementSize, BufferManager& bufferManager, WAL* wal)
        : BaseColumnOrList{structureIDAndFName, dataType, elementSize, bufferManager,
              true /*hasNULLBytes*/, wal} {};

    Column(const StorageStructureIDAndFName& structureIDAndFName, const common::DataType& dataType,
        BufferManager& bufferManager, WAL* wal)
        : Column(structureIDAndFName, dataType, common::Types::getDataTypeSize(dataType),
              bufferManager, wal){};

    // Expose for feature store
    void scan(const common::offset_t* nodeOffsets, size_t size, uint8_t* result);

    virtual void read(transaction::Transaction* transaction, common::ValueVector* nodeIDVector,
        common::ValueVector* resultVector);

    void writeValues(common::ValueVector* nodeIDVector, common::ValueVector* vectorToWriteFrom);

    bool isNull(common::offset_t nodeOffset, transaction::Transaction* transaction);
    void setNodeOffsetToNull(common::offset_t nodeOffset);

    // Currently, used only in CopyCSV tests.
    virtual common::Value readValueForTestingOnly(common::offset_t offset);

protected:
    void lookup(transaction::Transaction* transaction, common::ValueVector* nodeIDVector,
        common::ValueVector* resultVector, uint32_t vectorPos);

    virtual void lookup(transaction::Transaction* transaction, common::ValueVector* resultVector,
        uint32_t vectorPos, PageElementCursor& cursor);
    virtual inline void scan(transaction::Transaction* transaction,
        common::ValueVector* resultVector, PageElementCursor& cursor) {
        readBySequentialCopy(transaction, resultVector, cursor, identityMapper);
    }
    virtual void scanWithSelState(transaction::Transaction* transaction,
        common::ValueVector* resultVector, PageElementCursor& cursor) {
        readBySequentialCopyWithSelState(transaction, resultVector, cursor, identityMapper);
    }
    virtual void writeValueForSingleNodeIDPosition(common::offset_t nodeOffset,
        common::ValueVector* vectorToWriteFrom, uint32_t posInVectorToWriteFrom);
    WALPageIdxPosInPageAndFrame beginUpdatingPage(common::offset_t nodeOffset,
        common::ValueVector* vectorToWriteFrom, uint32_t posInVectorToWriteFrom);

private:
    // The reason why we make this function virtual is: we can't simply do memcpy on nodeIDs if
    // the adjColumn has tableIDCompression, in this case we only store the nodeOffset in
    // persistent store of adjColumn.
    virtual inline void writeToPage(WALPageIdxPosInPageAndFrame& walPageInfo,
        common::ValueVector* vectorToWriteFrom, uint32_t posInVectorToWriteFrom) {
        memcpy(walPageInfo.frame + mapElementPosToByteOffset(walPageInfo.posInPage),
            vectorToWriteFrom->getData() + getElemByteOffset(posInVectorToWriteFrom), elementSize);
    }
    // If necessary creates a second version (backed by the WAL) of a page that contains the fixed
    // length part of the value that will be written to.
    // Obtains *and does not release* the lock original page. Pins and updates the WAL version of
    // the page. Finally updates the page with the new value from vectorToWriteFrom.
    // Note that caller must ensure to unpin and release the WAL version of the page by calling
    // StorageStructure::unpinWALPageAndReleaseOriginalPageLock.
    WALPageIdxPosInPageAndFrame beginUpdatingPageAndWriteOnlyNullBit(
        common::offset_t nodeOffset, bool isNull);

protected:
    // no logical-physical page mapping is required for columns
    std::function<common::page_idx_t(common::page_idx_t)> identityMapper = [](uint32_t i) {
        return i;
    };
};

class PropertyColumnWithOverflow : public Column {
public:
    PropertyColumnWithOverflow(const StorageStructureIDAndFName& structureIDAndFNameOfMainColumn,
        const common::DataType& dataType, BufferManager& bufferManager, WAL* wal)
        : Column{structureIDAndFNameOfMainColumn, dataType, bufferManager, wal},
          diskOverflowFile{structureIDAndFNameOfMainColumn, bufferManager, wal} {}

    inline void read(transaction::Transaction* transaction, common::ValueVector* nodeIDVector,
        common::ValueVector* resultVector) override {
        resultVector->resetOverflowBuffer();
        Column::read(transaction, nodeIDVector, resultVector);
    }
    inline DiskOverflowFile* getDiskOverflowFile() { return &diskOverflowFile; }

    inline BMFileHandle* getDiskOverflowFileHandle() { return diskOverflowFile.getFileHandle(); }

protected:
    DiskOverflowFile diskOverflowFile;
};

class StringPropertyColumn : public PropertyColumnWithOverflow {

public:
    StringPropertyColumn(const StorageStructureIDAndFName& structureIDAndFNameOfMainColumn,
        const common::DataType& dataType, BufferManager& bufferManager, WAL* wal)
        : PropertyColumnWithOverflow{
              structureIDAndFNameOfMainColumn, dataType, bufferManager, wal} {};

    void writeValueForSingleNodeIDPosition(common::offset_t nodeOffset,
        common::ValueVector* vectorToWriteFrom, uint32_t posInVectorToWriteFrom) override;

    // Currently, used only in CopyCSV tests.
    common::Value readValueForTestingOnly(common::offset_t offset) override;

private:
    inline void lookup(transaction::Transaction* transaction, common::ValueVector* resultVector,
        uint32_t vectorPos, PageElementCursor& cursor) override {
        Column::lookup(transaction, resultVector, vectorPos, cursor);
        if (!resultVector->isNull(vectorPos)) {
            diskOverflowFile.scanSingleStringOverflow(
                transaction->getType(), *resultVector, vectorPos);
        }
    }
    inline void scan(transaction::Transaction* transaction, common::ValueVector* resultVector,
        PageElementCursor& cursor) override {
        Column::scan(transaction, resultVector, cursor);
        diskOverflowFile.scanStrings(transaction->getType(), *resultVector);
    }
    void scanWithSelState(transaction::Transaction* transaction, common::ValueVector* resultVector,
        PageElementCursor& cursor) override {
        Column::scanWithSelState(transaction, resultVector, cursor);
        diskOverflowFile.scanStrings(transaction->getType(), *resultVector);
    }
};

class ListPropertyColumn : public PropertyColumnWithOverflow {

public:
    ListPropertyColumn(const StorageStructureIDAndFName& structureIDAndFNameOfMainColumn,
        const common::DataType& dataType, BufferManager& bufferManager, WAL* wal)
        : PropertyColumnWithOverflow{
              structureIDAndFNameOfMainColumn, dataType, bufferManager, wal} {};

    void writeValueForSingleNodeIDPosition(common::offset_t nodeOffset,
        common::ValueVector* vectorToWriteFrom, uint32_t posInVectorToWriteFrom) override;

    common::Value readValueForTestingOnly(common::offset_t offset) override;

private:
    inline void lookup(transaction::Transaction* transaction, common::ValueVector* resultVector,
        uint32_t vectorPos, PageElementCursor& cursor) override {
        Column::lookup(transaction, resultVector, vectorPos, cursor);
        if (!resultVector->isNull(vectorPos)) {
            diskOverflowFile.scanSingleListOverflow(
                transaction->getType(), *resultVector, vectorPos);
        }
    }
    inline void scan(transaction::Transaction* transaction, common::ValueVector* resultVector,
        PageElementCursor& cursor) override {
        Column::scan(transaction, resultVector, cursor);
        diskOverflowFile.readListsToVector(transaction->getType(), *resultVector);
    }
    inline void scanWithSelState(transaction::Transaction* transaction,
        common::ValueVector* resultVector, PageElementCursor& cursor) override {
        Column::scanWithSelState(transaction, resultVector, cursor);
        diskOverflowFile.readListsToVector(transaction->getType(), *resultVector);
    }
};

class RelIDColumn : public Column {

public:
    RelIDColumn(const StorageStructureIDAndFName& structureIDAndFName, BufferManager& bufferManager,
        WAL* wal)
        : Column{structureIDAndFName, common::DataType(common::INTERNAL_ID),
              sizeof(common::offset_t), bufferManager, wal},
          commonTableID{structureIDAndFName.storageStructureID.columnFileID.relPropertyColumnID
                            .relNodeTableAndDir.relTableID} {
        assert(structureIDAndFName.storageStructureID.columnFileID.columnType ==
               ColumnType::REL_PROPERTY_COLUMN);
        assert(structureIDAndFName.storageStructureID.storageStructureType ==
               StorageStructureType::COLUMN);
    }

private:
    inline void lookup(transaction::Transaction* transaction, common::ValueVector* resultVector,
        uint32_t vectorPos, PageElementCursor& cursor) override {
        readInternalIDsFromAPageBySequentialCopy(transaction, resultVector, vectorPos,
            cursor.pageIdx, cursor.elemPosInPage, 1 /* numValuesToCopy */, commonTableID,
            false /* hasNoNullGuarantee */);
    }
    inline void scan(transaction::Transaction* transaction, common::ValueVector* resultVector,
        PageElementCursor& cursor) override {
        readInternalIDsBySequentialCopy(transaction, resultVector, cursor, identityMapper,
            commonTableID, false /* hasNoNullGuarantee */);
    }
    inline void scanWithSelState(transaction::Transaction* transaction,
        common::ValueVector* resultVector, PageElementCursor& cursor) override {
        readInternalIDsBySequentialCopyWithSelState(
            transaction, resultVector, cursor, identityMapper, commonTableID);
    }
    inline void writeToPage(WALPageIdxPosInPageAndFrame& walPageInfo,
        common::ValueVector* vectorToWriteFrom, uint32_t posInVectorToWriteFrom) override {
        auto relID = vectorToWriteFrom->getValue<common::relID_t>(posInVectorToWriteFrom);
        memcpy(walPageInfo.frame + mapElementPosToByteOffset(walPageInfo.posInPage), &relID.offset,
            sizeof(relID.offset));
    }

private:
    common::table_id_t commonTableID;
};

class AdjColumn : public Column {

public:
    AdjColumn(const StorageStructureIDAndFName& structureIDAndFName, common::table_id_t nbrTableID,
        BufferManager& bufferManager, WAL* wal)
        : Column{structureIDAndFName, common::DataType(common::INTERNAL_ID),
              sizeof(common::offset_t), bufferManager, wal},
          nbrTableID{nbrTableID} {};

private:
    inline void lookup(transaction::Transaction* transaction, common::ValueVector* resultVector,
        uint32_t vectorPos, PageElementCursor& cursor) override {
        readInternalIDsFromAPageBySequentialCopy(transaction, resultVector, vectorPos,
            cursor.pageIdx, cursor.elemPosInPage, 1 /* numValuesToCopy */, nbrTableID,
            false /* hasNoNullGuarantee */);
    }
    inline void scan(transaction::Transaction* transaction, common::ValueVector* resultVector,
        PageElementCursor& cursor) override {
        readInternalIDsBySequentialCopy(transaction, resultVector, cursor, identityMapper,
            nbrTableID, false /* hasNoNullGuarantee */);
    }
    inline void scanWithSelState(transaction::Transaction* transaction,
        common::ValueVector* resultVector, PageElementCursor& cursor) override {
        readInternalIDsBySequentialCopyWithSelState(
            transaction, resultVector, cursor, identityMapper, nbrTableID);
    }
    inline void writeToPage(WALPageIdxPosInPageAndFrame& walPageInfo,
        common::ValueVector* vectorToWriteFrom, uint32_t posInVectorToWriteFrom) override {
        *(walPageInfo.frame + mapElementPosToByteOffset(walPageInfo.posInPage)) =
            vectorToWriteFrom->getValue<common::nodeID_t>(posInVectorToWriteFrom).offset;
    }

private:
    common::table_id_t nbrTableID;
};

class ColumnFactory {

public:
    static std::unique_ptr<Column> getColumn(const StorageStructureIDAndFName& structureIDAndFName,
        const common::DataType& dataType, BufferManager& bufferManager, WAL* wal) {
        switch (dataType.typeID) {
        case common::INT64:
        case common::INT32:
        case common::INT16:
        case common::DOUBLE:
        case common::FLOAT:
        case common::BOOL:
        case common::DATE:
        case common::TIMESTAMP:
        case common::INTERVAL:
        case common::FIXED_LIST:
            return std::make_unique<Column>(structureIDAndFName, dataType, bufferManager, wal);
        case common::STRING:
            return std::make_unique<StringPropertyColumn>(
                structureIDAndFName, dataType, bufferManager, wal);
        case common::VAR_LIST:
            return std::make_unique<ListPropertyColumn>(
                structureIDAndFName, dataType, bufferManager, wal);
        case common::INTERNAL_ID:
            assert(structureIDAndFName.storageStructureID.storageStructureType ==
                   StorageStructureType::COLUMN);
            assert(structureIDAndFName.storageStructureID.columnFileID.columnType ==
                   ColumnType::REL_PROPERTY_COLUMN);
            return std::make_unique<RelIDColumn>(structureIDAndFName, bufferManager, wal);
        default:
            throw common::StorageException("Invalid type for property column creation.");
        }
    }
};

} // namespace storage
} // namespace kuzu
