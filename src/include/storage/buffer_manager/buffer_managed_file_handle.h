#pragma once

#include "storage/file_handle.h"

namespace kuzu {
namespace storage {

// BufferManagedFileHandle is a file handle that is backed by BufferManager. It holds the state of
// each in the file. File Handle is the bridge between a Column/Lists/Index and the Buffer Manager
// that abstracts the file in which that Column/Lists/Index is stored.
// BufferManagedFileHandle supports two types of files: versioned and non-versioned. Versioned files
// contains mapping from pages that have updates to the versioned pages in the wal file.
// Currently, only MemoryManager and WAL files are non-versioned.
class BufferManagedFileHandle : public FileHandle {
public:
    enum class FileVersionedType : uint8_t {
        VERSIONED_FILE = 0,    // The file is backed by versioned pages in wal file.
        NON_VERSIONED_FILE = 1 // The file does not have any versioned pages in wal file.
    };

    BufferManagedFileHandle(
        const std::string& path, uint8_t flags, FileVersionedType fileVersionedType);

    bool acquirePageLock(common::page_idx_t pageIdx, bool block);
    inline void releasePageLock(common::page_idx_t pageIdx) { pageLocks[pageIdx]->clear(); }

    void createPageVersionGroupIfNecessary(common::page_idx_t pageIdx);

    // This function is intended to be used after a fileInfo is created and we want the file
    // to have not pages and page locks. Should be called after ensuring that the buffer manager
    // does not hold any of the pages of the file.
    void resetToZeroPagesAndPageCapacity();
    void removePageIdxAndTruncateIfNecessary(common::page_idx_t pageIdx);

    bool hasWALPageVersionNoPageLock(common::page_idx_t pageIdx);
    void clearWALPageVersionIfNecessary(common::page_idx_t pageIdx);
    common::page_idx_t getWALPageVersionNoPageLock(common::page_idx_t pageIdx);
    void setWALPageVersion(common::page_idx_t originalPageIdx, common::page_idx_t pageIdxInWAL);
    void setWALPageVersionNoLock(common::page_idx_t pageIdx, common::page_idx_t pageVersion);

    inline common::page_idx_t getFrameIdx(common::page_idx_t pageIdx) {
        return pageIdxToFrameMap[pageIdx]->load();
    }
    inline void swizzle(common::page_idx_t pageIdx, common::page_idx_t swizzledVal) {
        pageIdxToFrameMap[pageIdx]->store(swizzledVal);
    }
    inline void unswizzle(common::page_idx_t pageIdx) {
        pageIdxToFrameMap[pageIdx]->store(UINT32_MAX);
    }

    static inline bool isAFrame(common::page_idx_t mappedFrameIdx) {
        return UINT32_MAX != mappedFrameIdx;
    }

private:
    void initPageIdxToFrameMapAndLocks();
    common::page_idx_t addNewPageWithoutLock() override;
    bool acquire(common::page_idx_t pageIdx);
    void removePageIdxAndTruncateIfNecessaryWithoutLock(common::page_idx_t pageIdxToRemove);
    void resizePageGroupLocksAndPageVersionsWithoutLock();
    uint32_t getNumPageGroups() {
        return ceil((double)numPages / common::StorageConstants::PAGE_GROUP_SIZE);
    }

private:
    FileVersionedType fileVersionedType;
    std::vector<std::unique_ptr<std::atomic_flag>> pageLocks;
    std::vector<std::unique_ptr<std::atomic<common::page_idx_t>>> pageIdxToFrameMap;
    std::vector<std::vector<common::page_idx_t>> pageVersions;
    std::vector<std::unique_ptr<std::atomic_flag>> pageGroupLocks;
};
} // namespace storage
} // namespace kuzu
