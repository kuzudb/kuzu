#include "storage/storage_structure/bm_backed_in_mem_column.h"

#include "common/constants.h"
#include "common/file_utils.h"
#include "storage/storage_utils.h"

namespace kuzu {
namespace storage {

BMBackedInMemColumn::BMBackedInMemColumn(std::string filePath, common::DataType dataType,
    uint16_t numBytesForElement, storage::BufferManager* bufferManager, uint64_t numElements)
    : filePath{std::move(filePath)}, dataType{std::move(dataType)},
      numBytesForElement{numBytesForElement}, bufferManager{bufferManager}, numElements{
                                                                                numElements} {
    numElementsInAPage = PageUtils::getNumElementsInAPage(numBytesForElement, true /* hasNull */);
    uint64_t numPages = ceil((double_t)numElements / (double_t)numElementsInAPage);
    pages.resize(numPages);
    nullMask = std::make_unique<common::NullMask>(numElements);
    nullMask->setAllNull();
    common::FileUtils::createFileWithSize(
        this->filePath, numPages * common::BufferPoolConstants::PAGE_4KB_SIZE);
    fileHandle =
        bufferManager->getBMFileHandle(this->filePath, FileHandle::O_PERSISTENT_FILE_NO_CREATE,
            storage::BMFileHandle::FileVersionedType::NON_VERSIONED_FILE);
}

void BMBackedInMemColumn::saveToFile() {
    auto nullEntriesOffset = numElementsInAPage * numBytesForElement;
    for (auto i = 0u; i < pages.size(); i++) {
        auto nullEntriesInPage =
            (uint64_t*)(bufferManager->pin(*fileHandle, i) + nullEntriesOffset);
        encodeNullBits(nullEntriesInPage, i);
        common::FileUtils::writeToFile(fileHandle->getFileInfo(), (uint8_t*)nullEntriesInPage,
            common::BufferPoolConstants::PAGE_4KB_SIZE - nullEntriesOffset,
            i * common::BufferPoolConstants::PAGE_4KB_SIZE + nullEntriesOffset);
        bufferManager->unpin(*fileHandle, i);
    }
}

void BMBackedInMemColumn::pinPagesForNodeOffsets(
    common::offset_t startOffset, common::offset_t endOffset) {
    auto startPageIdx = CursorUtils::getPageIdx(startOffset, numElementsInAPage);
    auto endPageIdx = CursorUtils::getPageIdx(endOffset, numElementsInAPage);
    for (auto i = startPageIdx; i <= endPageIdx; i++) {
        pages[i] = bufferManager->pin(*fileHandle, i);
    }
}

void BMBackedInMemColumn::unpinPagesForNodeOffsetRange(
    common::offset_t startOffset, common::offset_t endOffset) {
    auto startPageIdx = CursorUtils::getPageIdx(startOffset, numElementsInAPage);
    auto endPageIdx = CursorUtils::getPageIdx(endOffset, numElementsInAPage);
    for (auto i = startPageIdx; i <= endPageIdx; i++) {
        bufferManager->unpin(*fileHandle, i);
    }
}

void BMBackedInMemColumn::flushPagesForNodeOffsetRange(
    common::offset_t startOffset, common::offset_t endOffset) {
    auto startPageIdx = CursorUtils::getPageIdx(startOffset, numElementsInAPage);
    auto startPageOffset = (startOffset % numElementsInAPage) * numBytesForElement;
    auto endPageIdx = CursorUtils::getPageIdx(endOffset, numElementsInAPage);
    auto numBytesToWriteInEndPage = (endOffset % numElementsInAPage + 1) * numBytesForElement;
    auto fileInfo = fileHandle->getFileInfo();
    // Flush the first page (Note: we shouldn't flush the whole page to disk since data in the
    // range of [0, startPageOffset) will be populated/flushed by another task).
    common::FileUtils::writeToFile(fileInfo, pages[startPageIdx] + startPageOffset,
        common::BufferPoolConstants::PAGE_4KB_SIZE - startPageOffset,
        startPageIdx * common::BufferPoolConstants::PAGE_4KB_SIZE + startPageOffset);
    // Flush pages other than the first/last page (Note: we can directly flush a whole page to
    // disk since they are guaranteed not be shared by other tasks.).
    for (auto i = startPageIdx + 1; i < endPageIdx; i++) {
        common::FileUtils::writeToFile(fileInfo, pages[i],
            common::BufferPoolConstants::PAGE_4KB_SIZE,
            i * common::BufferPoolConstants::PAGE_4KB_SIZE);
    }
    // Flush the last page (Note: we shouldn't flush the whole page to disk since data in the
    // range of (numBytesToWriteInEndPage, PAGE_4KB_SIZE] will be populated/flushed by another
    // task).
    if (endPageIdx > startPageIdx) {
        common::FileUtils::writeToFile(fileInfo, pages[endPageIdx],
            numBytesToWriteInEndPage + numBytesForElement,
            endPageIdx * common::BufferPoolConstants::PAGE_4KB_SIZE);
    }
}

void BMBackedInMemColumn::setElement(common::offset_t offset, const uint8_t* val) {
    auto cursor = CursorUtils::getPageElementCursor(offset, numElementsInAPage);
    memcpy(
        pages[cursor.pageIdx] + cursor.elemPosInPage * numBytesForElement, val, numBytesForElement);
    if (nullMask != nullptr) {
        nullMask->setNull(offset, false);
    }
}

void BMBackedInMemColumn::encodeNullBits(uint64_t* nullEntries, common::page_idx_t pageIdx) {
    std::fill(nullEntries, nullEntries + common::BufferPoolConstants::PAGE_4KB_SIZE,
        common::NullMask::NO_NULL_ENTRY);
    auto startElemOffset = pageIdx * numElementsInAPage;
    auto numElementsInCurPage = std::min(numElementsInAPage, numElements - startElemOffset);
    for (auto i = 0u; i < numElementsInCurPage; i++) {
        if (nullMask->isNull(startElemOffset + i)) {
            auto entryPos = i >> common::NullMask::NUM_BITS_PER_NULL_ENTRY_LOG2;
            auto bitPosInEntry = i - (entryPos << common::NullMask::NUM_BITS_PER_NULL_ENTRY_LOG2);
            nullEntries[entryPos] |= common::NULL_BITMASKS_WITH_SINGLE_ONE[bitPosInEntry];
        }
    }
}

} // namespace storage
} // namespace kuzu
