#include "common/serializer/metadata_writer.h"

#include "storage/file_handle.h"
#include "storage/shadow_file.h"
#include "storage/shadow_utils.h"

namespace kuzu {
namespace common {

MetaWriter::MetaWriter(storage::MemoryManager* mm) : mm{mm}, pageOffset{0} {}

void MetaWriter::write(const uint8_t* data, uint64_t size) {
    auto remaining = size;
    while (remaining > 0) {
        if (needNewBuffer(size)) {
            const auto lastPage = pages.empty() ? nullptr : pages.back().get();
            if (lastPage) {
                auto toCopy = std::min(size, KUZU_PAGE_SIZE - pageOffset);
                memcpy(lastPage->getData() + pageOffset, data + (size - remaining), toCopy);
                remaining -= toCopy;
            }
            pages.push_back(mm->allocateBuffer(false, KUZU_PAGE_SIZE));
            pageOffset = 0;
        }
        auto toCopy = std::min(remaining, KUZU_PAGE_SIZE - pageOffset);
        memcpy(pages.back()->getData() + pageOffset, data + (size - remaining), toCopy);
        pageOffset += toCopy;
        remaining -= toCopy;
    }
}

storage::PageRange MetaWriter::flush(storage::FileHandle* fileHandle,
    storage::ShadowFile& shadowFile) const {
    auto numPagesToFlush = getNumPagesToFlush();
    auto pageManager = fileHandle->getPageManager();
    auto pageRange = pageManager->allocatePageRange(numPagesToFlush);
    flush(pageRange, fileHandle, shadowFile);
    return pageRange;
}

void MetaWriter::flush(storage::PageRange allocatedPageRange, storage::FileHandle* fileHandle,
    storage::ShadowFile& shadowFile) const {
    KU_ASSERT(allocatedPageRange.numPages >= getNumPagesToFlush());
    auto numPagesBeforeAllocate = allocatedPageRange.startPageIdx;
    for (auto i = 0u; i < getNumPagesToFlush(); i++) {
        auto pageIdx = allocatedPageRange.startPageIdx + i;
        auto insertingNewPage = pageIdx >= numPagesBeforeAllocate;
        auto shadowPageAndFrame = storage::ShadowUtils::createShadowVersionIfNecessaryAndPinPage(
            pageIdx, insertingNewPage, *fileHandle, shadowFile);
        memcpy(shadowPageAndFrame.frame, pages[i]->getData(), KUZU_PAGE_SIZE);
        shadowFile.getShadowingFH().unpinPage(shadowPageAndFrame.shadowPage);
    }
}

bool MetaWriter::needNewBuffer(uint64_t size) const {
    return pages.empty() || pageOffset + size > KUZU_PAGE_SIZE;
}

uint64_t MetaWriter::getPageSize() {
    return KUZU_PAGE_SIZE;
}

} // namespace common
} // namespace kuzu
