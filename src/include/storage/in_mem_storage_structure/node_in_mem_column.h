#pragma once

#include "common/types/types.h"
#include "storage/buffer_manager/buffer_manager.h"
#include "storage/storage_structure/in_mem_file.h"

namespace kuzu {
namespace storage {

class InMemColumnChunk {
public:
    InMemColumnChunk(common::offset_t startOffset, common::offset_t endOffset,
        uint16_t numBytesForElement, uint64_t numElementsInAPage)
        : numBytesForElement{numBytesForElement}, numElementsInAPage{numElementsInAPage} {
        startPageIdx = CursorUtils::getPageIdx(startOffset, numElementsInAPage);
        endPageIdx = CursorUtils::getPageIdx(endOffset, numElementsInAPage);
        auto numPages = endPageIdx - startPageIdx + 1;
        pages = std::make_unique<uint8_t[]>(numPages * common::BufferPoolConstants::PAGE_4KB_SIZE);
        memset(pages.get(), 0, numPages * common::BufferPoolConstants::PAGE_4KB_SIZE);
    }

    inline uint8_t* getPage(common::page_idx_t pageIdx) {
        assert(pageIdx <= endPageIdx && pageIdx >= startPageIdx);
        auto pageIdxInSet = pageIdx - startPageIdx;
        return pages.get() + (pageIdxInSet * common::BufferPoolConstants::PAGE_4KB_SIZE);
    }

    void copyValue(common::offset_t nodeOffset, const uint8_t* val) {
        auto cursor = CursorUtils::getPageElementCursor(nodeOffset, numElementsInAPage);
        auto page = getPage(cursor.pageIdx);
        auto elemPosInPageInBytes = cursor.elemPosInPage * numBytesForElement;
        memcpy(page + elemPosInPageInBytes, val, numBytesForElement);
    }

    uint8_t* getValue(common::offset_t nodeOffset) {
        auto cursor = CursorUtils::getPageElementCursor(nodeOffset, numElementsInAPage);
        auto elemPosInPageInBytes = cursor.elemPosInPage * numBytesForElement;
        return getPage(cursor.pageIdx) + elemPosInPageInBytes;
    }

private:
    uint16_t numBytesForElement;
    uint64_t numElementsInAPage;
    common::page_idx_t startPageIdx;
    common::page_idx_t endPageIdx;
    std::unique_ptr<uint8_t[]> pages;
};

class NodeInMemColumn {

public:
    NodeInMemColumn(std::string filePath, common::DataType dataType, uint16_t numBytesForElement,
        uint64_t numElements, uint64_t numBlocks);
    virtual ~NodeInMemColumn() = default;

    // Encode and flush null bits.
    virtual inline void saveToFile() { flushNullBits(); }

    inline common::DataType getDataType() { return dataType; }

    // Flush pages which holds nodeOffsets in the range [startOffset, endOffset] (inclusive).
    void flushChunk(
        InMemColumnChunk* chunk, common::offset_t startOffset, common::offset_t endOffset);

    void setElementInChunk(InMemColumnChunk* chunk, common::offset_t offset, const uint8_t* val);

    inline bool isNullAtNodeOffset(common::offset_t nodeOffset) {
        return nullMask->isNull(nodeOffset);
    }

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

class NodeInMemColumnWithOverflow : public NodeInMemColumn {

public:
    NodeInMemColumnWithOverflow(std::string filePath, common::DataType dataType,
        uint16_t numBytesForElement, uint64_t numElements, uint64_t numBlocks)
        : NodeInMemColumn{std::move(filePath), std::move(dataType), numBytesForElement, numElements,
              numBlocks} {
        assert(
            this->dataType.typeID == common::STRING || this->dataType.typeID == common::VAR_LIST);
        inMemOverflowFile =
            make_unique<InMemOverflowFile>(StorageUtils::getOverflowFileName(this->filePath));
    }

    inline InMemOverflowFile* getInMemOverflowFile() override { return inMemOverflowFile.get(); }

    void saveToFile() override {
        NodeInMemColumn::saveToFile();
        inMemOverflowFile->flush();
    }

protected:
    std::unique_ptr<InMemOverflowFile> inMemOverflowFile;
};

class InMemBMPageCollectionFactory {

public:
    static std::unique_ptr<NodeInMemColumn> getInMemBMPageCollection(const std::string& filePath,
        const common::DataType& dataType, uint64_t numElements, uint64_t numBlocks) {
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
            return make_unique<NodeInMemColumn>(filePath, dataType,
                common::Types::getDataTypeSize(dataType), numElements, numBlocks);
        case common::STRING:
        case common::VAR_LIST:
            return make_unique<NodeInMemColumnWithOverflow>(filePath, dataType,
                common::Types::getDataTypeSize(dataType), numElements, numBlocks);
        default:
            throw common::CopyException("Invalid type for property column creation.");
        }
    }
};

} // namespace storage
} // namespace kuzu
