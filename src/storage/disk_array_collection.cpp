#include "storage/disk_array_collection.h"

#include "common/system_config.h"
#include "common/types/types.h"
#include "storage/file_handle.h"
#include "storage/shadow_utils.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

DiskArrayCollection::DiskArrayCollection(FileHandle& fileHandle, ShadowFile& shadowFile,
    bool bypassShadowing)
    : fileHandle{fileHandle}, shadowFile{shadowFile}, bypassShadowing{bypassShadowing},
      numHeaders{0} {
    headersForReadTrx.push_back(std::make_unique<HeaderPage>());
    headersForWriteTrx.push_back(std::make_unique<HeaderPage>());
    headerPagesOnDisk = 0;
}

DiskArrayCollection::DiskArrayCollection(FileHandle& fileHandle, ShadowFile& shadowFile,
    page_idx_t firstHeaderPage, bool bypassShadowing)
    : fileHandle{fileHandle}, shadowFile{shadowFile}, bypassShadowing{bypassShadowing},
      numHeaders{0} {
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
    } while (headerPageIdx != INVALID_PAGE_IDX);
    headerPagesOnDisk = headersForReadTrx.size();
}

void DiskArrayCollection::checkpoint(page_idx_t firstHeaderPage) {
    // Write headers to disk
    page_idx_t headerPage = firstHeaderPage;
    for (page_idx_t indexInMemory = 0; indexInMemory < headersForWriteTrx.size(); indexInMemory++) {
        // Only update if the headers for the given page have changed
        // Or if the page has not yet been written
        if (indexInMemory >= headerPagesOnDisk ||
            *headersForWriteTrx[indexInMemory] != *headersForReadTrx[indexInMemory]) {
            ShadowUtils::updatePage(fileHandle, headerPage, true /*writing full page*/, shadowFile,
                [&](auto* frame) {
                    memcpy(frame, headersForWriteTrx[indexInMemory].get(), sizeof(HeaderPage));
                    if constexpr (sizeof(HeaderPage) < KUZU_PAGE_SIZE) {
                        // Zero remaining data in the page
                        std::fill(frame + sizeof(HeaderPage), frame + KUZU_PAGE_SIZE, 0);
                    }
                });
        }
        headerPage = headersForWriteTrx[indexInMemory]->nextHeaderPage;
    }
    headerPagesOnDisk = headersForWriteTrx.size();
}

size_t DiskArrayCollection::addDiskArray() {
    auto oldSize = numHeaders++;
    // This may not be the last header page. If we rollback there may be header pages which are
    // empty
    auto pageIdx = numHeaders % HeaderPage::NUM_HEADERS_PER_PAGE;
    if (pageIdx >= headersForWriteTrx.size()) {
        auto nextHeaderPage = fileHandle.getPageManager()->allocatePage();
        headersForWriteTrx.back()->nextHeaderPage = nextHeaderPage;
        // We can't really roll back the structural changes in the PKIndex (the disk arrays are
        // created in the destructor and there are a fixed number which does not change after that
        // point), so we apply those to the version that would otherwise be identical to the one on
        // disk
        headersForReadTrx.back()->nextHeaderPage = nextHeaderPage;

        headersForWriteTrx.emplace_back(std::make_unique<HeaderPage>());
        // Also add a new read header page as we need to pass read headers to the disk arrays
        // Newly added read headers will be empty until checkpointing
        headersForReadTrx.emplace_back(std::make_unique<HeaderPage>());
    }

    auto& headerPage = *headersForWriteTrx[pageIdx];
    KU_ASSERT(headerPage.numHeaders < HeaderPage::NUM_HEADERS_PER_PAGE);
    auto indexInPage = headerPage.numHeaders;
    headerPage.headers[indexInPage] = DiskArrayHeader();
    headerPage.numHeaders++;
    headersForReadTrx[pageIdx]->numHeaders++;
    return oldSize;
}

void DiskArrayCollection::reclaimStorage(PageManager& pageManager,
    common::page_idx_t firstHeaderPage) const {
    page_idx_t headerPage = firstHeaderPage;
    for (page_idx_t indexInMemory = 0; indexInMemory < headersForReadTrx.size(); indexInMemory++) {
        if (headerPage != INVALID_PAGE_IDX) {
            pageManager.freePage(headerPage);

            headerPage = headersForReadTrx[indexInMemory]->nextHeaderPage;
        }
    }
}

} // namespace storage
} // namespace kuzu
