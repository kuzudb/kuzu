#include "common/serializer/metadata_writer.h"

#include "storage/file_handle.h"
#include "storage/shadow_utils.h"
#include "storage/wal/shadow_file.h"

namespace kuzu {
namespace common {

MetadataWriter::MetadataWriter(storage::MemoryManager* mm, storage::FileHandle* fileHandle)
    : mm{mm}, fileHandle{fileHandle}, bufferOffset{0} {}

void MetadataWriter::write(const uint8_t* data, uint64_t size) {
    auto remaining = size;
    while (remaining > 0) {
        if (needNewBuffer(size)) {
            const auto lastBuffer = buffers.empty() ? nullptr : buffers.back().get();
            if (lastBuffer) {
                auto toCopy = std::min(size, lastBuffer->getBuffer().size() - bufferOffset);
                memcpy(lastBuffer->getData() + bufferOffset, data + (size - remaining), toCopy);
                remaining -= toCopy;
            }
            buffers.push_back(mm->allocateBuffer(false, KUZU_PAGE_SIZE));
            bufferOffset = 0;
        }
        auto toCopy = std::min(remaining, buffers.back()->getBuffer().size() - bufferOffset);
        memcpy(buffers.back()->getData() + bufferOffset, data + (size - remaining), toCopy);
        bufferOffset += toCopy;
        remaining -= toCopy;
    }
}

void MetadataWriter::flush(storage::ShadowFile& shadowFile) const {
    auto numPagesToFlush = buffers.size();
    auto pageManager = fileHandle->getPageManager();
    auto numPages = fileHandle->getNumPages();
    auto pageRange = pageManager->allocatePageRange(numPagesToFlush);
    for (auto i = 0u; i < pageRange.numPages; i++) {
        auto pageIdx = pageRange.startPageIdx + i;
        auto insertingNewPage = pageIdx > numPages;
        auto shadowPageAndFrame = storage::ShadowUtils::createShadowVersionIfNecessaryAndPinPage(
            pageIdx, insertingNewPage, *fileHandle, storage::DBFileID::newDataFileID(), shadowFile);
        memcpy(shadowPageAndFrame.frame, buffers[i]->getData(), KUZU_PAGE_SIZE);
        shadowFile.getShadowingFH().unpinPage(shadowPageAndFrame.shadowPage);
    }
}

bool MetadataWriter::needNewBuffer(uint64_t size) const {
    return buffers.empty() || bufferOffset + size > buffers.back()->getBuffer().size();
}

} // namespace common
} // namespace kuzu
