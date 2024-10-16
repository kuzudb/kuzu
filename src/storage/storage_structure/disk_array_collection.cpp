#include "storage/storage_structure/disk_array_collection.h"

#include "common/constants.h"
#include "common/types/types.h"
#include "storage//file_handle.h"
#include "storage/shadow_utils.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

DiskArrayCollection::DiskArrayCollection(FileHandle& fileHandle, DBFileID dbFileID,
    ShadowFile& shadowFile, page_idx_t firstHeaderPage, bool bypassShadowing)
    : fileHandle{fileHandle}, dbFileID{dbFileID}, shadowFile{shadowFile},
      bypassShadowing{bypassShadowing}, headerPageIndices{firstHeaderPage}, numHeaders{0} {
    if (fileHandle.getNumPages() > firstHeaderPage) {
        // Read headers from disk
        page_idx_t headerPageIdx = firstHeaderPage;
        do {
            fileHandle.optimisticReadPage(headerPageIdx, [&](auto* frame) {
                const auto page = reinterpret_cast<HeaderPage*>(frame);
                headersForReadTrx.push_back(std::make_unique<HeaderPage>(*page));
                headersForWriteTrx.push_back(std::make_unique<HeaderPage>(*page));
                headerPageIdx = page->nextHeaderPage;
                numHeaders += page->numHeaders;
            });
            if (headerPageIdx != INVALID_PAGE_IDX) {
                headerPageIndices.push_back(headerPageIdx);
            }
        } while (headerPageIdx != INVALID_PAGE_IDX);
        headerPagesOnDisk = headersForReadTrx.size();
    } else {
        KU_ASSERT(fileHandle.getNumPages() == firstHeaderPage);
        // Reserve the first header page
        fileHandle.addNewPage();
        headersForReadTrx.push_back(std::make_unique<HeaderPage>());
        headersForWriteTrx.push_back(std::make_unique<HeaderPage>());
        headerPagesOnDisk = 0;
    }
}

void DiskArrayCollection::checkpoint() {
    // Write headers to disk
    size_t indexInMemory = 0;
    auto headerPageIdx = headerPageIndices.begin();
    do {
        KU_ASSERT(headerPageIdx != headerPageIndices.end());
        // Only update if the headers for the given page have changed
        // Or if the page has not yet been written
        KU_ASSERT(indexInMemory < headersForWriteTrx.size());
        if (indexInMemory >= headerPagesOnDisk ||
            *headersForWriteTrx[indexInMemory] != *headersForReadTrx[indexInMemory]) {
            ShadowUtils::updatePage(fileHandle, dbFileID, *headerPageIdx,
                true /*writing full page*/, shadowFile, [&](auto* frame) {
                    memcpy(frame, headersForWriteTrx[indexInMemory].get(), sizeof(HeaderPage));
                    if constexpr (sizeof(HeaderPage) < KUZU_PAGE_SIZE) {
                        // Zero remaining data in the page
                        std::fill(frame + sizeof(HeaderPage), frame + KUZU_PAGE_SIZE, 0);
                    }
                });
        }
        indexInMemory++;
        headerPageIdx++;
    } while (indexInMemory < headersForWriteTrx.size());
}

size_t DiskArrayCollection::addDiskArray() {
    auto oldSize = numHeaders++;
    if (headersForReadTrx.empty() ||
        headersForWriteTrx.back()->numHeaders == HeaderPage::NUM_HEADERS_PER_PAGE) {
        auto nextHeaderPage = fileHandle.addNewPage();
        if (!headersForWriteTrx.empty()) {
            headersForWriteTrx.back()->nextHeaderPage = nextHeaderPage;
        }
        headerPageIndices.push_back(nextHeaderPage);

        headersForWriteTrx.emplace_back(std::make_unique<HeaderPage>());
        // Also add a new read header page as we need to pass read headers to the disk arrays
        // Newly added read headers will be empty until checkpointing
        headersForReadTrx.emplace_back(std::make_unique<HeaderPage>());
    }

    KU_ASSERT(headersForWriteTrx.back()->numHeaders < HeaderPage::NUM_HEADERS_PER_PAGE);
    auto indexInPage = headersForWriteTrx.back()->numHeaders;
    headersForWriteTrx.back()->headers[indexInPage] = DiskArrayHeader();
    headersForWriteTrx.back()->numHeaders++;
    return oldSize;
}

} // namespace storage
} // namespace kuzu
