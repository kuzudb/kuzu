#pragma once

#include "storage/buffer_manager/vm_region.h"
#include "storage/file_handle.h"

namespace kuzu {
namespace storage {

static constexpr uint64_t IS_IN_FRAME_MASK = 0x8000000000000000;
static constexpr uint64_t DIRTY_MASK = 0x4000000000000000;
static constexpr uint64_t PAGE_IDX_MASK = 0x3FFFFFFFFFFFFFFF;

enum class LockMode : uint8_t { SPIN = 0, NON_BLOCKING = 1 };

class BMFileHandle;
class BufferManager;

// Keeps the state information of a page in a file.
class PageState {
public:
    inline bool isInFrame() const { return pageIdx & IS_IN_FRAME_MASK; }
    inline void setDirty() { pageIdx |= DIRTY_MASK; }
    inline void clearDirty() { pageIdx &= ~DIRTY_MASK; }
    inline bool isDirty() const { return pageIdx & DIRTY_MASK; }
    inline common::page_idx_t getPageIdx() const {
        return (common::page_idx_t)(pageIdx & PAGE_IDX_MASK);
    }
    inline uint64_t incrementPinCount() { return pinCount.fetch_add(1); }
    inline uint64_t decrementPinCount() { return pinCount.fetch_sub(1); }
    inline void setPinCount(uint64_t newPinCount) { pinCount.store(newPinCount); }
    inline uint64_t getPinCount() const { return pinCount.load(); }
    inline uint64_t getEvictionTimestamp() const { return evictionTimestamp.load(); }
    inline uint64_t incrementEvictionTimestamp() { return evictionTimestamp.fetch_add(1); }
    inline void releaseLock() { lock.clear(); }

    bool acquireLock(LockMode lockMode);
    void setInFrame(common::page_idx_t pageIdx);
    void resetState();

private:
    std::atomic_flag lock = ATOMIC_FLAG_INIT;
    // Highest 1st bit indicates if this page is loaded or not, 2nd bit indicates if this
    // page is dirty or not. The rest 62 bits records the page idx inside the file.
    uint64_t pageIdx = 0;
    std::atomic<uint32_t> pinCount = 0;
    std::atomic<uint64_t> evictionTimestamp = 0;
};

// This class is used to keep the WAL page idxes of a page group in the original file handle.
// For each original page in the page group, it has a corresponding WAL page idx. The WAL page idx
// is initialized as INVALID_PAGE_IDX to indicate that the page does not have any updates in WAL
// file. To synchronize accesses to the WAL version page idxes, we use a lock for each WAL page idx.
class WALPageIdxGroup {
public:
    WALPageIdxGroup();

    // `originalPageIdxInGroup` is the page idx of the original page within the page group.
    // For example, given a page idx `x` in a file, its page group id is `x / PAGE_GROUP_SIZE`, and
    // the pageIdxInGroup is `x % PAGE_GROUP_SIZE`.
    inline void acquireWALPageIdxLock(common::page_idx_t originalPageIdxInGroup) {
        walPageIdxLocks[originalPageIdxInGroup]->lock();
    }
    inline void releaseWALPageIdxLock(common::page_idx_t originalPageIdxInGroup) {
        walPageIdxLocks[originalPageIdxInGroup]->unlock();
    }
    inline common::page_idx_t getWALVersionPageIdxNoLock(common::page_idx_t pageIdxInGroup) const {
        return walPageIdxes[pageIdxInGroup];
    }
    inline void setWALVersionPageIdxNoLock(
        common::page_idx_t pageIdxInGroup, common::page_idx_t walVersionPageIdx) {
        walPageIdxes[pageIdxInGroup] = walVersionPageIdx;
    }

private:
    std::vector<common::page_idx_t> walPageIdxes;
    std::vector<std::unique_ptr<std::mutex>> walPageIdxLocks;
};

// BMFileHandle is a file handle that is backed by BufferManager. It holds the state of
// each page in the file. File Handle is the bridge between a Column/Lists/Index and the Buffer
// Manager that abstracts the file in which that Column/Lists/Index is stored.
// BMFileHandle supports two types of files: versioned and non-versioned. Versioned files
// contains mapping from pages that have updates to the versioned pages in the wal file.
// Currently, only MemoryManager and WAL files are non-versioned.
class BMFileHandle : public FileHandle {
    friend class BufferManager;

public:
    enum class FileVersionedType : uint8_t {
        VERSIONED_FILE = 0,    // The file is backed by versioned pages in wal file.
        NON_VERSIONED_FILE = 1 // The file does not have any versioned pages in wal file.
    };

    BMFileHandle(const std::string& path, uint8_t flags, BufferManager* bm,
        common::PageSizeClass pageSizeClass, FileVersionedType fileVersionedType);

    common::page_group_idx_t addWALPageIdxGroupIfNecessary(common::page_idx_t originalPageIdx);
    // This function is intended to be used after a fileInfo is created and we want the file
    // to have no pages and page locks. Should be called after ensuring that the buffer manager
    // does not hold any of the pages of the file.
    void resetToZeroPagesAndPageCapacity();
    void removePageIdxAndTruncateIfNecessary(common::page_idx_t pageIdx);

    void acquireWALPageIdxLock(common::page_idx_t originalPageIdx);
    void releaseWALPageIdxLock(common::page_idx_t originalPageIdx);

    // Return true if the original page's page group has a WAL page group.
    bool hasWALPageGroup(common::page_group_idx_t originalPageIdx);

    // This function assumes that the caller has already acquired the wal page idx lock.
    // Return true if the page has a WAL page idx, whose value is not equal to INVALID_PAGE_IDX.
    bool hasWALPageVersionNoWALPageIdxLock(common::page_idx_t originalPageIdx);

    void clearWALPageIdxIfNecessary(common::page_idx_t originalPageIdx);
    common::page_idx_t getWALPageIdxNoWALPageIdxLock(common::page_idx_t originalPageIdx);
    void setWALPageIdx(common::page_idx_t originalPageIdx, common::page_idx_t pageIdxInWAL);
    // This function assumes that the caller has already acquired the wal page idx lock.
    void setWALPageIdxNoLock(common::page_idx_t originalPageIdx, common::page_idx_t pageIdxInWAL);

private:
    inline PageState* getPageState(common::page_idx_t pageIdx) {
        assert(pageIdx < numPages && pageStates[pageIdx]);
        return pageStates[pageIdx].get();
    }
    inline void clearPageState(common::page_idx_t pageIdx) {
        assert(pageIdx < numPages && pageStates[pageIdx]);
        pageStates[pageIdx]->resetState();
    }
    inline bool acquirePageLock(common::page_idx_t pageIdx, LockMode lockMode) {
        return getPageState(pageIdx)->acquireLock(lockMode);
    }
    inline void releasePageLock(common::page_idx_t pageIdx) {
        getPageState(pageIdx)->releaseLock();
    }
    inline common::frame_idx_t getFrameIdx(common::page_idx_t pageIdx) {
        assert(pageIdx < pageCapacity);
        return (frameGroupIdxes[pageIdx >> common::StorageConstants::PAGE_GROUP_SIZE_LOG2]
                   << common::StorageConstants::PAGE_GROUP_SIZE_LOG2) |
               (pageIdx & common::StorageConstants::PAGE_IDX_IN_GROUP_MASK);
    }
    inline common::PageSizeClass getPageSizeClass() const { return pageSizeClass; }

    void initPageStatesAndGroups();
    common::page_idx_t addNewPageWithoutLock() override;
    void addNewPageGroupWithoutLock();
    inline common::page_group_idx_t getNumPageGroups() {
        return ceil((double)numPages / common::StorageConstants::PAGE_GROUP_SIZE);
    }

private:
    FileVersionedType fileVersionedType;
    BufferManager* bm;
    common::PageSizeClass pageSizeClass;
    std::vector<std::unique_ptr<PageState>> pageStates;
    // Each file page group corresponds to a frame group in the VMRegion.
    std::vector<common::page_group_idx_t> frameGroupIdxes;
    // For each page group, if it has any WAL page version, we keep a `WALPageIdxGroup` in this map.
    // `WALPageIdxGroup` records the WAL page idx for each page in the page group.
    // Accesses to this map is synchronized by `fhSharedMutex`.
    std::unordered_map<common::page_group_idx_t, std::unique_ptr<WALPageIdxGroup>> walPageIdxGroups;
};
} // namespace storage
} // namespace kuzu
