#pragma once

#include <cstdint>

#include "common/constants.h"
#include "common/types/types.h"
#include "storage/storage_structure/disk_array.h"

namespace kuzu {
namespace storage {

class BufferManager;
class WAL;
class BMFileHandle;

class DiskArrayCollection {
    struct HeaderPage {
        explicit HeaderPage(uint32_t numHeaders = 0)
            : headers{}, nextHeaderPage{common::INVALID_PAGE_IDX}, numHeaders{numHeaders} {}
        static constexpr size_t NUM_HEADERS_PER_PAGE =
            (common::BufferPoolConstants::PAGE_4KB_SIZE - sizeof(common::page_idx_t) -
                sizeof(uint32_t)) /
            sizeof(DiskArrayHeader);

        bool operator==(const HeaderPage&) const = default;

        std::array<DiskArrayHeader, NUM_HEADERS_PER_PAGE> headers;
        common::page_idx_t nextHeaderPage;
        uint32_t numHeaders;
    };
    static_assert(std::has_unique_object_representations_v<HeaderPage>);

public:
    DiskArrayCollection(BMFileHandle& fileHandle, DBFileID dbFileID, BufferManager* bufferManager,
        WAL* wal, common::page_idx_t firstHeaderPage = 0, bool bypassWAL = false);

    void prepareCommit();

    void checkpointInMemory() {
        for (size_t i = 0; i < headersForWriteTrx.size(); i++) {
            *headersForReadTrx[i] = *headersForWriteTrx[i];
        }
        headerPagesOnDisk = headersForReadTrx.size();
    }

    void rollbackInMemory() {
        for (size_t i = 0; i < headersForWriteTrx.size(); i++) {
            *headersForWriteTrx[i] = *headersForReadTrx[i];
        }
    }

    template<typename T>
    std::unique_ptr<DiskArray<T>> getDiskArray(uint32_t idx) {
        KU_ASSERT(idx < numHeaders);
        auto& readHeader = headersForReadTrx[idx / HeaderPage::NUM_HEADERS_PER_PAGE]
                               ->headers[idx % HeaderPage::NUM_HEADERS_PER_PAGE];
        auto& writeHeader = headersForWriteTrx[idx / HeaderPage::NUM_HEADERS_PER_PAGE]
                                ->headers[idx % HeaderPage::NUM_HEADERS_PER_PAGE];
        return std::make_unique<DiskArray<T>>(fileHandle, dbFileID, readHeader, writeHeader,
            &bufferManager, &wal, bypassWAL);
    }

    size_t addDiskArray();

private:
    BMFileHandle& fileHandle;
    DBFileID dbFileID;
    BufferManager& bufferManager;
    WAL& wal;
    bool bypassWAL;
    common::page_idx_t headerPagesOnDisk;
    std::vector<std::unique_ptr<HeaderPage>> headersForReadTrx;
    std::vector<std::unique_ptr<HeaderPage>> headersForWriteTrx;
    // List of indices used to store old header pages.
    std::vector<common::page_idx_t> headerPageIndices;
    uint64_t numHeaders;
};

} // namespace storage
} // namespace kuzu
