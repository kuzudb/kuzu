#pragma once

#include "common/types/types.h"
#include "storage/buffer_manager/buffer_manager.h"
#include "storage/storage_structure/in_mem_file.h"

namespace kuzu {
namespace storage {

class BMBackedInMemColumn {

public:
    BMBackedInMemColumn(std::string filePath, common::DataType dataType,
        uint16_t numBytesForElement, storage::BufferManager* bufferManager, uint64_t numElements);

    // Encode and flush null bits.
    virtual void saveToFile();

    inline common::DataType getDataType() { return dataType; }

    // Pin pages which holds nodeOffsets in the range [startOffset, endOffset] (inclusive).
    void pinPagesForNodeOffsets(common::offset_t startOffset, common::offset_t endOffset);

    // Unpin pages which holds nodeOffsets in the range [startOffset, endOffset] (inclusive).
    void unpinPagesForNodeOffsetRange(common::offset_t startOffset, common::offset_t endOffset);

    // Flush pages which holds nodeOffsets in the range [startOffset, endOffset] (inclusive).
    void flushPagesForNodeOffsetRange(common::offset_t startOffset, common::offset_t endOffset);

    void setElement(common::offset_t offset, const uint8_t* val);

    inline bool isNullAtNodeOffset(common::offset_t nodeOffset) {
        return nullMask->isNull(nodeOffset);
    }

    template<typename T>
    std::unique_ptr<T> getElement(common::offset_t offset) {
        auto element = std::make_unique<T>();
        auto cursor = CursorUtils::getPageElementCursor(offset, numElementsInAPage);
        auto frame = bufferManager->pin(*fileHandle, cursor.pageIdx);
        // Note: we can't directly return a frame that is managed by BM after unpin. We should
        // save the element in a temporary buffer.
        memcpy(
            element.get(), frame + cursor.elemPosInPage * numBytesForElement, numBytesForElement);
        bufferManager->unpin(*fileHandle, cursor.pageIdx);
        return element;
    }

    virtual ~BMBackedInMemColumn() = default;

private:
    void encodeNullBits(uint64_t* nullEntries, common::page_idx_t pageIdx);

protected:
    std::string filePath;
    uint16_t numBytesForElement;
    uint64_t numElementsInAPage;
    std::vector<uint8_t*> pages;
    BufferManager* bufferManager;
    std::unique_ptr<BMFileHandle> fileHandle;
    std::unique_ptr<common::NullMask> nullMask;
    common::DataType dataType;
    uint64_t numElements;
};

class BMBackedInMemColumnWithOverflow : public BMBackedInMemColumn {

public:
    BMBackedInMemColumnWithOverflow(std::string filePath, common::DataType dataType,
        uint16_t numBytesForElement, storage::BufferManager* bufferManager, uint64_t numElements)
        : BMBackedInMemColumn{std::move(filePath), std::move(dataType), numBytesForElement,
              bufferManager, numElements} {
        assert(
            this->dataType.typeID == common::STRING || this->dataType.typeID == common::VAR_LIST);
        inMemOverflowFile =
            make_unique<InMemOverflowFile>(StorageUtils::getOverflowFileName(this->filePath));
    }

    inline InMemOverflowFile* getInMemOverflowFile() { return inMemOverflowFile.get(); }

    void saveToFile() override {
        BMBackedInMemColumn::saveToFile();
        inMemOverflowFile->flush();
    }

protected:
    std::unique_ptr<InMemOverflowFile> inMemOverflowFile;
};

class InMemBMPageCollectionFactory {

public:
    static std::unique_ptr<BMBackedInMemColumn> getInMemBMPageCollection(std::string filePath,
        common::DataType dataType, uint64_t numElements, storage::BufferManager* bufferManager) {
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
            return make_unique<BMBackedInMemColumn>(filePath, dataType,
                common::Types::getDataTypeSize(dataType), bufferManager, numElements);
        case common::STRING:
        case common::VAR_LIST:
            return make_unique<BMBackedInMemColumnWithOverflow>(filePath, dataType,
                common::Types::getDataTypeSize(dataType), bufferManager, numElements);
        default:
            throw common::CopyException("Invalid type for property column creation.");
        }
    }
};

} // namespace storage
} // namespace kuzu
