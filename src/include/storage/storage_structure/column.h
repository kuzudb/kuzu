#pragma once

#include "catalog/catalog.h"
#include "common/types/value.h"
#include "storage/storage_structure/disk_overflow_file.h"
#include "storage/storage_structure/storage_structure.h"

namespace kuzu {
namespace storage {

using read_data_func_t = std::function<void(transaction::Transaction* transaction, uint8_t* frame,
    PageElementCursor& pageCursor, common::ValueVector* resultVector, uint32_t posInVector,
    uint32_t numValuesToRead, DiskOverflowFile* diskOverflowFile)>;
using write_data_func_t = std::function<void(uint8_t* frame, uint16_t posInFrame,
    common::ValueVector* vector, uint32_t posInVector, DiskOverflowFile* diskOverflowFile)>;

class NullColumn;

class Column : public BaseColumnOrList {
public:
    // Currently extended by SERIAL column.
    explicit Column(const common::LogicalType& dataType) : BaseColumnOrList{dataType} {};

    Column(const StorageStructureIDAndFName& structureIDAndFName,
        const common::LogicalType& dataType, BufferManager* bufferManager, WAL* wal)
        : Column(structureIDAndFName, dataType, storage::StorageUtils::getDataTypeSize(dataType),
              bufferManager, wal){};

    Column(const StorageStructureIDAndFName& structureIDAndFName,
        const common::LogicalType& dataType, size_t elementSize, BufferManager* bufferManager,
        WAL* wal, bool requireNullBits = true);

    // Expose for feature store
    virtual void batchLookup(const common::offset_t* nodeOffsets, size_t size, uint8_t* result);

    virtual void read(transaction::Transaction* transaction, common::ValueVector* nodeIDVector,
        common::ValueVector* resultVector);

    void write(common::ValueVector* nodeIDVector, common::ValueVector* vectorToWriteFrom);

    bool isNull(common::offset_t nodeOffset, transaction::Transaction* transaction);
    void setNull(common::offset_t nodeOffset);

    inline NullColumn* getNullColumn() { return nullColumn.get(); }

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
    static void readValuesFromPage(transaction::Transaction* transaction, uint8_t* frame,
        PageElementCursor& pageCursor, common::ValueVector* resultVector, uint32_t posInVector,
        uint32_t numValuesToRead, DiskOverflowFile* diskOverflowFile);
    static void writeValueToPage(uint8_t* frame, uint16_t posInFrame, common::ValueVector* vector,
        uint32_t posInVector, DiskOverflowFile* diskOverflowFile);

protected:
    // no logical-physical page mapping is required for columns
    std::function<common::page_idx_t(common::page_idx_t)> identityMapper = [](uint32_t i) {
        return i;
    };

    read_data_func_t readDataFunc;
    write_data_func_t writeDataFunc;
    std::unique_ptr<DiskOverflowFile> diskOverflowFile;
    std::unique_ptr<NullColumn> nullColumn;
};

class NullColumn : public Column {
public:
    NullColumn(const StorageStructureIDAndFName& structureIDAndFName, BufferManager* bufferManager,
        WAL* wal)
        : Column{structureIDAndFName, common::LogicalType(common::LogicalTypeID::BOOL),
              sizeof(bool), bufferManager, wal, false /* requireNullBits */} {
        readDataFunc = NullColumn::readNullsFromPage;
    }

    void write(common::offset_t nodeOffset, common::ValueVector* vectorToWriteFrom,
        uint32_t posInVectorToWriteFrom) final;

    bool readValue(common::offset_t nodeOffset, transaction::Transaction* transaction);
    void setValue(common::offset_t nodeOffset, bool isNull = true);

private:
    static void readNullsFromPage(transaction::Transaction* transaction, uint8_t* frame,
        PageElementCursor& pageCursor, common::ValueVector* resultVector, uint32_t posInVector,
        uint32_t numValuesToRead, DiskOverflowFile* diskOverflowFile);
};

class PropertyColumnWithOverflow : public Column {
public:
    PropertyColumnWithOverflow(const StorageStructureIDAndFName& structureIDAndFNameOfMainColumn,
        const common::LogicalType& dataType, BufferManager* bufferManager, WAL* wal)
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
        const common::LogicalType& dataType, BufferManager* bufferManager, WAL* wal)
        : PropertyColumnWithOverflow{
              structureIDAndFNameOfMainColumn, dataType, bufferManager, wal} {
        writeDataFunc = StringPropertyColumn::writeStringToPage;
    };

    // Currently, used only in CopyCSV tests.
    common::Value readValueForTestingOnly(common::offset_t offset) final;

private:
    inline void lookup(transaction::Transaction* transaction, common::offset_t nodeOffset,
        common::ValueVector* resultVector, uint32_t vectorPos) final {
        resultVector->resetAuxiliaryBuffer();
        Column::lookup(transaction, nodeOffset, resultVector, vectorPos);
        if (!resultVector->isNull(vectorPos)) {
            diskOverflowFile->scanSingleStringOverflow(
                transaction->getType(), *resultVector, vectorPos);
        }
    }
    inline void scan(transaction::Transaction* transaction, common::ValueVector* nodeIDVector,
        common::ValueVector* resultVector) final {
        resultVector->resetAuxiliaryBuffer();
        Column::scan(transaction, nodeIDVector, resultVector);
        diskOverflowFile->scanStrings(transaction->getType(), *resultVector);
    }

    static void writeStringToPage(uint8_t* frame, uint16_t posInFrame, common::ValueVector* vector,
        uint32_t posInVector, DiskOverflowFile* diskOverflowFile);
};

class ListPropertyColumn : public PropertyColumnWithOverflow {
public:
    ListPropertyColumn(const StorageStructureIDAndFName& structureIDAndFNameOfMainColumn,
        const common::LogicalType& dataType, BufferManager* bufferManager, WAL* wal)
        : PropertyColumnWithOverflow{
              structureIDAndFNameOfMainColumn, dataType, bufferManager, wal} {
        readDataFunc = ListPropertyColumn::readListsFromPage;
        writeDataFunc = ListPropertyColumn::writeListToPage;
    };

    common::Value readValueForTestingOnly(common::offset_t offset) final;

private:
    static void readListsFromPage(transaction::Transaction* transaction, uint8_t* frame,
        PageElementCursor& pageCursor, common::ValueVector* resultVector, uint32_t posInVector,
        uint32_t numValuesToRead, DiskOverflowFile* diskOverflowFile);
    static void writeListToPage(uint8_t* frame, uint16_t posInFrame, common::ValueVector* vector,
        uint32_t posInVector, DiskOverflowFile* diskOverflowFile);
};

class StructPropertyColumn : public Column {
public:
    StructPropertyColumn(const StorageStructureIDAndFName& structureIDAndFName,
        const common::LogicalType& dataType, BufferManager* bufferManager, WAL* wal);

    void read(transaction::Transaction* transaction, common::ValueVector* nodeIDVector,
        common::ValueVector* resultVector) final;

private:
    std::vector<std::unique_ptr<Column>> structFieldColumns;
};

class InternalIDColumn : public Column {
public:
    InternalIDColumn(const StorageStructureIDAndFName& structureIDAndFName,
        BufferManager* bufferManager, WAL* wal)
        : Column{structureIDAndFName, common::LogicalType(common::LogicalTypeID::INTERNAL_ID),
              sizeof(common::offset_t), bufferManager, wal, true /* requireNullBits */} {
        readDataFunc = InternalIDColumn::readInternalIDsFromPage;
        writeDataFunc = InternalIDColumn::writeInternalIDToPage;
    }

private:
    static void readInternalIDsFromPage(transaction::Transaction* transaction, uint8_t* frame,
        PageElementCursor& pageCursor, common::ValueVector* resultVector, uint32_t posInVector,
        uint32_t numValuesToRead, DiskOverflowFile* diskOverflowFile);
    static void writeInternalIDToPage(uint8_t* frame, uint16_t posInFrame,
        common::ValueVector* vector, uint32_t posInVector, DiskOverflowFile* diskOverflowFile);
};

class SerialColumn : public Column {
public:
    SerialColumn() : Column{common::LogicalType{common::LogicalTypeID::SERIAL}} {}

    void read(transaction::Transaction* transaction, common::ValueVector* nodeIDVector,
        common::ValueVector* resultVector) final;
};

class ColumnFactory {
public:
    static std::unique_ptr<Column> getColumn(const StorageStructureIDAndFName& structureIDAndFName,
        const common::LogicalType& logicalType, BufferManager* bufferManager, WAL* wal) {
        switch (logicalType.getLogicalTypeID()) {
        case common::LogicalTypeID::INT64:
        case common::LogicalTypeID::INT32:
        case common::LogicalTypeID::INT16:
        case common::LogicalTypeID::DOUBLE:
        case common::LogicalTypeID::FLOAT:
        case common::LogicalTypeID::BOOL:
        case common::LogicalTypeID::DATE:
        case common::LogicalTypeID::TIMESTAMP:
        case common::LogicalTypeID::INTERVAL:
        case common::LogicalTypeID::FIXED_LIST:
            return std::make_unique<Column>(structureIDAndFName, logicalType, bufferManager, wal);
        case common::LogicalTypeID::BLOB:
        case common::LogicalTypeID::STRING:
            return std::make_unique<StringPropertyColumn>(
                structureIDAndFName, logicalType, bufferManager, wal);
        case common::LogicalTypeID::VAR_LIST:
            return std::make_unique<ListPropertyColumn>(
                structureIDAndFName, logicalType, bufferManager, wal);
        case common::LogicalTypeID::INTERNAL_ID:
            return std::make_unique<InternalIDColumn>(structureIDAndFName, bufferManager, wal);
        case common::LogicalTypeID::STRUCT:
            return std::make_unique<StructPropertyColumn>(
                structureIDAndFName, logicalType, bufferManager, wal);
        case common::LogicalTypeID::SERIAL:
            return std::make_unique<SerialColumn>();
        default:
            throw common::StorageException("Invalid type for property column creation.");
        }
    }
};

} // namespace storage
} // namespace kuzu
