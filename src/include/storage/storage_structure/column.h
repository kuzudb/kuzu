#pragma once

#include "catalog/catalog.h"
#include "common/types/value.h"
#include "storage/storage_structure/disk_overflow_file.h"
#include "storage/storage_structure/storage_structure.h"

namespace kuzu {
namespace storage {

using scan_data_func_t = std::function<void(transaction::Transaction* transaction, uint8_t* frame,
    PageElementCursor& pageCursor, common::ValueVector* resultVector, uint32_t posInVector,
    uint32_t numElementsPerPage, uint32_t numValuesToRead, common::table_id_t commonTableID,
    DiskOverflowFile* diskOverflowFile)>;
using lookup_data_func_t = std::function<void(transaction::Transaction* transaction, uint8_t* frame,
    PageElementCursor& pageCursor, common::ValueVector* resultVector, uint32_t posInVector,
    uint32_t numElementsPerPage, common::table_id_t commonTableID,
    DiskOverflowFile* diskOverflowFile)>;

class Column : public BaseColumnOrList {
public:
    // TODO(Guodong): Clean up column constructors.
    // Currently extended by SERIAL column.
    explicit Column(const common::DataType& dataType)
        : BaseColumnOrList{dataType}, tableID{common::INVALID_TABLE_ID} {};

    Column(const StorageStructureIDAndFName& structureIDAndFName, const common::DataType& dataType,
        BufferManager* bufferManager, WAL* wal)
        : Column(structureIDAndFName, dataType, common::Types::getDataTypeSize(dataType),
              bufferManager, wal){};

    Column(const StorageStructureIDAndFName& structureIDAndFName, const common::DataType& dataType,
        size_t elementSize, BufferManager* bufferManager, WAL* wal)
        : Column{structureIDAndFName, dataType, elementSize, bufferManager, wal,
              common::INVALID_TABLE_ID} {};

    // Extended by INTERNAL_ID column.
    Column(const StorageStructureIDAndFName& structureIDAndFName, const common::DataType& dataType,
        size_t elementSize, BufferManager* bufferManager, WAL* wal, common::table_id_t tableID)
        : BaseColumnOrList{structureIDAndFName, dataType, elementSize, bufferManager,
              true /*hasNULLBytes*/, wal},
          tableID{tableID} {
        scanDataFunc = Column::scanValuesFromPage;
        lookupDataFunc = Column::lookupValueFromPage;
    }

    // Expose for feature store
    virtual void batchLookup(const common::offset_t* nodeOffsets, size_t size, uint8_t* result);

    virtual void read(transaction::Transaction* transaction, common::ValueVector* nodeIDVector,
        common::ValueVector* resultVector);

    void writeValues(common::ValueVector* nodeIDVector, common::ValueVector* vectorToWriteFrom);

    bool isNull(common::offset_t nodeOffset, transaction::Transaction* transaction);
    void setNull(common::offset_t nodeOffset);

    // Currently, used only in CopyCSV tests.
    // TODO(Guodong): Remove this function. Use `read` instead.
    virtual common::Value readValueForTestingOnly(common::offset_t offset);

protected:
    void lookup(transaction::Transaction* transaction, common::ValueVector* nodeIDVector,
        common::ValueVector* resultVector, uint32_t vectorPos);

    virtual void lookup(transaction::Transaction* transaction, common::offset_t nodeOffset,
        common::ValueVector* resultVector, uint32_t vectorPos);
    virtual void scan(transaction::Transaction* transaction, common::ValueVector* nodeIDVector,
        common::ValueVector* resultVector);
    virtual void writeValueForSingleNodeIDPosition(common::offset_t nodeOffset,
        common::ValueVector* vectorToWriteFrom, uint32_t posInVectorToWriteFrom);
    WALPageIdxPosInPageAndFrame beginUpdatingPage(common::offset_t nodeOffset,
        common::ValueVector* vectorToWriteFrom, uint32_t posInVectorToWriteFrom);

    void readFromPage(transaction::Transaction* transaction, common::page_idx_t pageIdx,
        const std::function<void(uint8_t*)>& func);

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

    static void scanValuesFromPage(transaction::Transaction* transaction, uint8_t* frame,
        PageElementCursor& pageCursor, common::ValueVector* resultVector, uint32_t posInVector,
        uint32_t numElementsPerPage, uint32_t numValuesToRead, common::table_id_t commonTableID,
        DiskOverflowFile* diskOverflowFile);
    static void lookupValueFromPage(transaction::Transaction* transaction, uint8_t* frame,
        PageElementCursor& pageCursor, common::ValueVector* resultVector, uint32_t posInVector,
        uint32_t numElementsPerPage, common::table_id_t commonTableID,
        DiskOverflowFile* diskOverflowFile);

protected:
    // no logical-physical page mapping is required for columns
    std::function<common::page_idx_t(common::page_idx_t)> identityMapper = [](uint32_t i) {
        return i;
    };

    scan_data_func_t scanDataFunc;
    lookup_data_func_t lookupDataFunc;
    common::table_id_t tableID;
    std::unique_ptr<DiskOverflowFile> diskOverflowFile;
};

class PropertyColumnWithOverflow : public Column {
public:
    PropertyColumnWithOverflow(const StorageStructureIDAndFName& structureIDAndFNameOfMainColumn,
        const common::DataType& dataType, BufferManager* bufferManager, WAL* wal)
        : Column{structureIDAndFNameOfMainColumn, dataType, bufferManager, wal} {
        diskOverflowFile =
            std::make_unique<DiskOverflowFile>(structureIDAndFNameOfMainColumn, bufferManager, wal);
    }

    inline DiskOverflowFile* getDiskOverflowFile() { return diskOverflowFile.get(); }

    inline BMFileHandle* getDiskOverflowFileHandle() { return diskOverflowFile->getFileHandle(); }
};

class StringPropertyColumn : public PropertyColumnWithOverflow {
public:
    StringPropertyColumn(const StorageStructureIDAndFName& structureIDAndFNameOfMainColumn,
        const common::DataType& dataType, BufferManager* bufferManager, WAL* wal)
        : PropertyColumnWithOverflow{
              structureIDAndFNameOfMainColumn, dataType, bufferManager, wal} {};

    void writeValueForSingleNodeIDPosition(common::offset_t nodeOffset,
        common::ValueVector* vectorToWriteFrom, uint32_t posInVectorToWriteFrom) override;

    // Currently, used only in CopyCSV tests.
    common::Value readValueForTestingOnly(common::offset_t offset) override;

private:
    inline void lookup(transaction::Transaction* transaction, common::offset_t nodeOffset,
        common::ValueVector* resultVector, uint32_t vectorPos) override {
        common::StringVector::resetOverflowBuffer(resultVector);
        Column::lookup(transaction, nodeOffset, resultVector, vectorPos);
        if (!resultVector->isNull(vectorPos)) {
            diskOverflowFile->scanSingleStringOverflow(
                transaction->getType(), *resultVector, vectorPos);
        }
    }
    inline void scan(transaction::Transaction* transaction, common::ValueVector* nodeIDVector,
        common::ValueVector* resultVector) override {
        common::StringVector::resetOverflowBuffer(resultVector);
        Column::scan(transaction, nodeIDVector, resultVector);
        diskOverflowFile->scanStrings(transaction->getType(), *resultVector);
    }
};

class ListPropertyColumn : public PropertyColumnWithOverflow {
public:
    ListPropertyColumn(const StorageStructureIDAndFName& structureIDAndFNameOfMainColumn,
        const common::DataType& dataType, BufferManager* bufferManager, WAL* wal)
        : PropertyColumnWithOverflow{
              structureIDAndFNameOfMainColumn, dataType, bufferManager, wal} {
        scanDataFunc = ListPropertyColumn::scanListsFromPage;
        lookupDataFunc = ListPropertyColumn::lookupListFromPage;
    };

    void writeValueForSingleNodeIDPosition(common::offset_t nodeOffset,
        common::ValueVector* vectorToWriteFrom, uint32_t posInVectorToWriteFrom) override;

    common::Value readValueForTestingOnly(common::offset_t offset) override;

private:
    static void scanListsFromPage(transaction::Transaction* transaction, uint8_t* frame,
        PageElementCursor& pageCursor, common::ValueVector* resultVector, uint32_t posInVector,
        uint32_t numElementsPerPage, uint32_t numValuesToRead, common::table_id_t commonTableID,
        DiskOverflowFile* diskOverflowFile);
    static void lookupListFromPage(transaction::Transaction* transaction, uint8_t* frame,
        PageElementCursor& pageCursor, common::ValueVector* resultVector, uint32_t posInVector,
        uint32_t numElementsPerPage, common::table_id_t commonTableID,
        DiskOverflowFile* diskOverflowFile);
};

class StructPropertyColumn : public Column {
public:
    StructPropertyColumn(const StorageStructureIDAndFName& structureIDAndFName,
        const common::DataType& dataType, BufferManager* bufferManager, WAL* wal);

    void read(transaction::Transaction* transaction, common::ValueVector* nodeIDVector,
        common::ValueVector* resultVector) override;

private:
    std::vector<std::unique_ptr<Column>> structFieldColumns;
};

class InternalIDColumn : public Column {
public:
    InternalIDColumn(const StorageStructureIDAndFName& structureIDAndFName,
        BufferManager* bufferManager, WAL* wal, common::table_id_t tableID)
        : Column{structureIDAndFName, common::DataType(common::INTERNAL_ID),
              sizeof(common::offset_t), bufferManager, wal, tableID} {
        scanDataFunc = InternalIDColumn::scanInternalIDsFromPage;
        lookupDataFunc = InternalIDColumn::lookupInternalIDFromPage;
    }

private:
    inline void writeToPage(WALPageIdxPosInPageAndFrame& walPageInfo,
        common::ValueVector* vectorToWriteFrom, uint32_t posInVectorToWriteFrom) override {
        auto relID = vectorToWriteFrom->getValue<common::relID_t>(posInVectorToWriteFrom);
        memcpy(walPageInfo.frame + mapElementPosToByteOffset(walPageInfo.posInPage), &relID.offset,
            sizeof(relID.offset));
    }

    static void scanInternalIDsFromPage(transaction::Transaction* transaction, uint8_t* frame,
        PageElementCursor& pageCursor, common::ValueVector* resultVector, uint32_t posInVector,
        uint32_t numElementsPerPage, uint32_t numValuesToRead, common::table_id_t commonTableID,
        DiskOverflowFile* diskOverflowFile);
    static void lookupInternalIDFromPage(transaction::Transaction* transaction, uint8_t* frame,
        PageElementCursor& pageCursor, common::ValueVector* resultVector, uint32_t posInVector,
        uint32_t numElementsPerPage, common::table_id_t commonTableID,
        DiskOverflowFile* diskOverflowFile);
};

class AdjColumn : public InternalIDColumn {
public:
    AdjColumn(const StorageStructureIDAndFName& structureIDAndFName, common::table_id_t nbrTableID,
        BufferManager* bufferManager, WAL* wal)
        : InternalIDColumn{structureIDAndFName, bufferManager, wal, nbrTableID} {}
};

class SerialColumn : public Column {
public:
    SerialColumn() : Column{common::DataType{common::SERIAL}} {}

    void read(transaction::Transaction* transaction, common::ValueVector* nodeIDVector,
        common::ValueVector* resultVector) override;
};

class ColumnFactory {
public:
    static std::unique_ptr<Column> getColumn(const StorageStructureIDAndFName& structureIDAndFName,
        const common::DataType& dataType, BufferManager* bufferManager, WAL* wal) {
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
            // RelID column in rel tables.
            assert(structureIDAndFName.storageStructureID.storageStructureType ==
                       StorageStructureType::COLUMN &&
                   structureIDAndFName.storageStructureID.columnFileID.columnType ==
                       ColumnType::REL_PROPERTY_COLUMN);
            return std::make_unique<InternalIDColumn>(structureIDAndFName, bufferManager, wal,
                structureIDAndFName.storageStructureID.columnFileID.relPropertyColumnID
                    .relNodeTableAndDir.relTableID);
        case common::STRUCT:
            return std::make_unique<StructPropertyColumn>(
                structureIDAndFName, dataType, bufferManager, wal);
        case common::SERIAL:
            return std::make_unique<SerialColumn>();
        default:
            throw common::StorageException("Invalid type for property column creation.");
        }
    }
};

} // namespace storage
} // namespace kuzu
