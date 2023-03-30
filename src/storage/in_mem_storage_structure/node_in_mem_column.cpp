#include "storage/in_mem_storage_structure/node_in_mem_column.h"

#include "common/constants.h"
#include "common/file_utils.h"
#include "storage/storage_utils.h"

namespace kuzu {
namespace storage {

NodeInMemColumn::NodeInMemColumn(std::string filePath, common::DataType dataType,
    uint16_t numBytesForElement, uint64_t numElements, uint64_t numBlocks)
    : filePath{std::move(filePath)}, dataType{std::move(dataType)},
      numBytesForElement{numBytesForElement}, numElements{numElements} {
    numElementsInAPage = PageUtils::getNumElementsInAPage(numBytesForElement, true /* hasNull */);
    nullEntriesOffset = numElementsInAPage * numBytesForElement;
    numNullEntriesPerPage = (numElementsInAPage + common::NullMask::NUM_BITS_PER_NULL_ENTRY - 1) /
                            common::NullMask::NUM_BITS_PER_NULL_ENTRY;
    numPages = ceil((double_t)numElements / (double_t)numElementsInAPage);
    nullMask = std::make_unique<common::NullMask>(numPages * numElementsInAPage);
    nullMask->setAllNull();
    common::FileUtils::createFileWithSize(
        this->filePath, numPages * common::BufferPoolConstants::PAGE_4KB_SIZE);
    fileHandle = std::make_unique<FileHandle>(this->filePath, O_WRONLY);
}

void NodeInMemColumn::flushChunk(
    InMemColumnChunk* chunk, common::offset_t startOffset, common::offset_t endOffset) {
    auto firstPageIdx = CursorUtils::getPageIdx(startOffset, numElementsInAPage);
    auto firstPageOffset = (startOffset % numElementsInAPage) * numBytesForElement;
    auto lastPageIdx = CursorUtils::getPageIdx(endOffset, numElementsInAPage);
    auto numBytesToWriteInLastPage = (endOffset % numElementsInAPage + 1) * numBytesForElement;
    auto fileInfo = fileHandle->getFileInfo();
    // Flush the first page (Note: we shouldn't flush the whole page to disk since data in the
    // range of [0, startPageOffset) will be populated/flushed by another task).
    common::FileUtils::writeToFile(fileInfo, chunk->getValue(startOffset),
        common::BufferPoolConstants::PAGE_4KB_SIZE - firstPageOffset,
        (firstPageIdx * common::BufferPoolConstants::PAGE_4KB_SIZE) + firstPageOffset);
    // Flush pages other than the first/last page (Note: we can directly flush a whole page to
    // disk since they are guaranteed not be shared by other tasks.).
    for (auto i = firstPageIdx + 1; i < lastPageIdx; i++) {
        common::FileUtils::writeToFile(fileInfo, chunk->getPage(i),
            common::BufferPoolConstants::PAGE_4KB_SIZE,
            i * common::BufferPoolConstants::PAGE_4KB_SIZE);
    }
    // Flush the last page (Note: we shouldn't flush the whole page to disk since data in the
    // range of (numBytesToWriteInEndPage, PAGE_4KB_SIZE] will be populated/flushed by another
    // task).
    if (lastPageIdx > firstPageIdx) {
        common::FileUtils::writeToFile(fileInfo, chunk->getPage(lastPageIdx),
            numBytesToWriteInLastPage, lastPageIdx * common::BufferPoolConstants::PAGE_4KB_SIZE);
    }
}

void NodeInMemColumn::setElementInChunk(
    InMemColumnChunk* chunk, common::offset_t offset, const uint8_t* val) {
    chunk->copyValue(offset, val);
    if (nullMask != nullptr) {
        nullMask->setNull(offset, false);
    }
}

std::unique_ptr<uint64_t[]> NodeInMemColumn::encodeNullBits(common::page_idx_t pageIdx) {
    auto startElemOffset = pageIdx * numElementsInAPage;
    auto nullEntries = std::make_unique<uint64_t[]>(numNullEntriesPerPage);
    std::fill(nullEntries.get(), nullEntries.get() + numNullEntriesPerPage,
        common::NullMask::NO_NULL_ENTRY);
    for (auto i = 0u; i < numElementsInAPage; i++) {
        if (nullMask->isNull(startElemOffset + i)) {
            auto entryPos = i >> common::NullMask::NUM_BITS_PER_NULL_ENTRY_LOG2;
            auto bitPosInEntry = i - (entryPos << common::NullMask::NUM_BITS_PER_NULL_ENTRY_LOG2);
            nullEntries[entryPos] |= common::NULL_BITMASKS_WITH_SINGLE_ONE[bitPosInEntry];
        }
    }
    return nullEntries;
}

void NodeInMemColumn::flushNullBits() {
    auto maxPageIdx = numPages - 1;
    for (auto pageIdx = 0u; pageIdx < maxPageIdx; pageIdx++) {
        auto startElemOffset = pageIdx * numElementsInAPage;
        bool hasNullElements = false;
        for (auto elemIdx = 0u; elemIdx < numElementsInAPage; elemIdx++) {
            if (nullMask->isNull(startElemOffset + elemIdx)) {
                hasNullElements = true;
                break;
            }
        }
        if (hasNullElements) {
            auto nullEntries = encodeNullBits(pageIdx);
            common::FileUtils::writeToFile(fileHandle->getFileInfo(), (uint8_t*)nullEntries.get(),
                numNullEntriesPerPage * common::NullMask::NUM_BYTES_PER_NULL_ENTRY,
                pageIdx * common::BufferPoolConstants::PAGE_4KB_SIZE + nullEntriesOffset);
        }
    }
    auto nullEntries = encodeNullBits(maxPageIdx);
    common::FileUtils::writeToFile(fileHandle->getFileInfo(), (uint8_t*)nullEntries.get(),
        numNullEntriesPerPage * common::NullMask::NUM_BYTES_PER_NULL_ENTRY,
        maxPageIdx * common::BufferPoolConstants::PAGE_4KB_SIZE + nullEntriesOffset);
}

} // namespace storage
} // namespace kuzu
