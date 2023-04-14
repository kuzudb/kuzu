#pragma once

#include "storage/in_mem_storage_structure/in_mem_column_chunk.h"

namespace kuzu {
namespace storage {
// TODO(GUODONG): Currently, we have both InMemNodeColumn and InMemColumn. This is a temporary
// solution for now to allow gradual refactorings. Eventually, we should only have InMemColumn.
class InMemNodeColumn {
public:
    InMemNodeColumn(std::string filePath, common::DataType dataType, uint16_t numBytesForElement,
        uint64_t numElements);
    virtual ~InMemNodeColumn() = default;

    // Encode and flush null bits.
    virtual inline void saveToFile() { flushNullBits(); }

    inline common::DataType getDataType() { return dataType; }

    // Flush pages which holds nodeOffsets in the range [startOffset, endOffset] (inclusive).
    void flushChunk(
        InMemColumnChunk* chunk, common::offset_t startOffset, common::offset_t endOffset);

    void setElementInChunk(InMemColumnChunk* chunk, common::offset_t offset, const uint8_t* val);

    virtual inline InMemOverflowFile* getInMemOverflowFile() { return nullptr; }

    inline common::NullMask* getNullMask() { return nullMask.get(); }
    inline uint16_t getNumBytesForElement() const { return numBytesForElement; }
    inline uint64_t getNumElementsInAPage() const { return numElementsInAPage; }

private:
    std::unique_ptr<uint64_t[]> encodeNullBits(common::page_idx_t pageIdx);
    void flushNullBits();

protected:
    std::string filePath;
    uint16_t numBytesForElement;
    uint64_t numElementsInAPage;
    uint64_t nullEntriesOffset;
    uint64_t numNullEntriesPerPage;
    std::unique_ptr<FileHandle> fileHandle;
    std::unique_ptr<common::NullMask> nullMask;
    common::DataType dataType;
    uint64_t numElements;
    uint64_t numPages;
};

class NodeInMemColumnWithOverflow : public InMemNodeColumn {

public:
    NodeInMemColumnWithOverflow(std::string filePath, common::DataType dataType,
        uint16_t numBytesForElement, uint64_t numElements)
        : InMemNodeColumn{
              std::move(filePath), std::move(dataType), numBytesForElement, numElements} {
        assert(
            this->dataType.typeID == common::STRING || this->dataType.typeID == common::VAR_LIST);
        inMemOverflowFile =
            make_unique<InMemOverflowFile>(StorageUtils::getOverflowFileName(this->filePath));
    }

    inline InMemOverflowFile* getInMemOverflowFile() override { return inMemOverflowFile.get(); }

    void saveToFile() override {
        InMemNodeColumn::saveToFile();
        inMemOverflowFile->flush();
    }

protected:
    std::unique_ptr<InMemOverflowFile> inMemOverflowFile;
};

class NodeInMemColumnFactory {
public:
    static std::unique_ptr<InMemNodeColumn> getNodeInMemColumn(
        const std::string& filePath, const common::DataType& dataType, uint64_t numElements) {
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
            return make_unique<InMemNodeColumn>(
                filePath, dataType, common::Types::getDataTypeSize(dataType), numElements);
        case common::STRING:
        case common::VAR_LIST:
            return make_unique<NodeInMemColumnWithOverflow>(
                filePath, dataType, common::Types::getDataTypeSize(dataType), numElements);
        default:
            throw common::CopyException("Invalid type for property column creation.");
        }
    }
};

} // namespace storage
} // namespace kuzu
