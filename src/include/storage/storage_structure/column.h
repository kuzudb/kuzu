#pragma once

#include "catalog/catalog.h"
#include "common/types/value.h"
#include "storage/storage_structure/disk_overflow_file.h"
#include "storage/storage_structure/storage_structure.h"

namespace kuzu {
namespace storage {

using scan_data_func_t = std::function<void(transaction::Transaction* transaction, uint8_t* frame,
    PageElementCursor& pageCursor, common::ValueVector* resultVector, uint32_t posInVector,
    uint32_t numElementsPerPage, uint32_t numValuesToRead, DiskOverflowFile* diskOverflowFile)>;
using lookup_data_func_t = std::function<void(transaction::Transaction* transaction, uint8_t* frame,
    PageElementCursor& pageCursor, common::ValueVector* resultVector, uint32_t posInVector,
    uint32_t numElementsPerPage, DiskOverflowFile* diskOverflowFile)>;
using write_data_func_t = std::function<void(uint8_t* frame, uint16_t posInFrame,
    common::ValueVector* vector, uint32_t posInVector, DiskOverflowFile* diskOverflowFile)>;

class Column : public BaseColumnOrList {
public:
    // Currently extended by SERIAL column.
    explicit Column(const common::DataType& dataType) : BaseColumnOrList{dataType} {};

    Column(const StorageStructureIDAndFName& structureIDAndFName, const common::DataType& dataType,
        BufferManager* bufferManager, WAL* wal)
        : Column(structureIDAndFName, dataType, common::Types::getDataTypeSize(dataType),
              bufferManager, wal){};

    Column(const StorageStructureIDAndFName& structureIDAndFName, const common::DataType& dataType,
        size_t elementSize, BufferManager* bufferManager, WAL* wal)
        : BaseColumnOrList{structureIDAndFName, dataType, elementSize, bufferManager,
              true /*hasNULLBytes*/, wal} {
        scanDataFunc = Column::scanValuesFromPage;
        lookupDataFunc = Column::lookupValueFromPage;
        writeDataFunc = Column::writeValueToPage;
    }

    // Expose for feature store
    virtual void batchLookup(const common::offset_t* nodeOffsets, size_t size, uint8_t* result);

    virtual void read(transaction::Transaction* transaction, common::ValueVector* nodeIDVector,
        common::ValueVector* resultVector);

    void write(common::ValueVector* nodeIDVector, common::ValueVector* vectorToWriteFrom);

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
    virtual void write(common::offset_t nodeOffset, common::ValueVector* vectorToWriteFrom,
        uint32_t posInVectorToWriteFrom);

    void readFromPage(transaction::Transaction* transaction, common::page_idx_t pageIdx,
        const std::function<void(uint8_t*)>& func);

private:
    static void scanValuesFromPage(transaction::Transaction* transaction, uint8_t* frame,
        PageElementCursor& pageCursor, common::ValueVector* resultVector, uint32_t posInVector,
        uint32_t numElementsPerPage, uint32_t numValuesToRead, DiskOverflowFile* diskOverflowFile);
    static void lookupValueFromPage(transaction::Transaction* transaction, uint8_t* frame,
        PageElementCursor& pageCursor, common::ValueVector* resultVector, uint32_t posInVector,
        uint32_t numElementsPerPage, DiskOverflowFile* diskOverflowFile);
    static void writeValueToPage(uint8_t* frame, uint16_t posInFrame, common::ValueVector* vector,
        uint32_t posInVector, DiskOverflowFile* diskOverflowFile);

protected:
    // no logical-physical page mapping is required for columns
    std::function<common::page_idx_t(common::page_idx_t)> identityMapper = [](uint32_t i) {
        return i;
    };

    scan_data_func_t scanDataFunc;
    lookup_data_func_t lookupDataFunc;
    write_data_func_t writeDataFunc;
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
              structureIDAndFNameOfMainColumn, dataType, bufferManager, wal} {
        writeDataFunc = StringPropertyColumn::writeStringToPage;
    };

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
    static void writeStringToPage(uint8_t* frame, uint16_t posInFrame, common::ValueVector* vector,
        uint32_t posInVector, DiskOverflowFile* diskOverflowFile);
};

class ListPropertyColumn : public PropertyColumnWithOverflow {
public:
    ListPropertyColumn(const StorageStructureIDAndFName& structureIDAndFNameOfMainColumn,
        const common::DataType& dataType, BufferManager* bufferManager, WAL* wal)
        : PropertyColumnWithOverflow{
              structureIDAndFNameOfMainColumn, dataType, bufferManager, wal} {
        scanDataFunc = ListPropertyColumn::scanListsFromPage;
        lookupDataFunc = ListPropertyColumn::lookupListFromPage;
        writeDataFunc = ListPropertyColumn::writeListToPage;
    };

    common::Value readValueForTestingOnly(common::offset_t offset) override;

private:
    static void scanListsFromPage(transaction::Transaction* transaction, uint8_t* frame,
        PageElementCursor& pageCursor, common::ValueVector* resultVector, uint32_t posInVector,
        uint32_t numElementsPerPage, uint32_t numValuesToRead, DiskOverflowFile* diskOverflowFile);
    static void lookupListFromPage(transaction::Transaction* transaction, uint8_t* frame,
        PageElementCursor& pageCursor, common::ValueVector* resultVector, uint32_t posInVector,
        uint32_t numElementsPerPage, DiskOverflowFile* diskOverflowFile);
    static void writeListToPage(uint8_t* frame, uint16_t posInFrame, common::ValueVector* vector,
        uint32_t posInVector, DiskOverflowFile* diskOverflowFile);
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
        BufferManager* bufferManager, WAL* wal)
        : Column{structureIDAndFName, common::DataType(common::INTERNAL_ID),
              sizeof(common::offset_t), bufferManager, wal} {
        scanDataFunc = InternalIDColumn::scanInternalIDsFromPage;
        lookupDataFunc = InternalIDColumn::lookupInternalIDFromPage;
        writeDataFunc = InternalIDColumn::writeInternalIDToPage;
    }

private:
    static void scanInternalIDsFromPage(transaction::Transaction* transaction, uint8_t* frame,
        PageElementCursor& pageCursor, common::ValueVector* resultVector, uint32_t posInVector,
        uint32_t numElementsPerPage, uint32_t numValuesToRead, DiskOverflowFile* diskOverflowFile);
    static void lookupInternalIDFromPage(transaction::Transaction* transaction, uint8_t* frame,
        PageElementCursor& pageCursor, common::ValueVector* resultVector, uint32_t posInVector,
        uint32_t numElementsPerPage, DiskOverflowFile* diskOverflowFile);
    static void writeInternalIDToPage(uint8_t* frame, uint16_t posInFrame,
        common::ValueVector* vector, uint32_t posInVector, DiskOverflowFile* diskOverflowFile);
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
            return std::make_unique<InternalIDColumn>(structureIDAndFName, bufferManager, wal);
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
