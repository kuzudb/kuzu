#pragma once

#include <atomic>
#include <cmath>

#include "storage/buffer_manager/vm_region.h"
#include "storage/file_handle.h"

namespace kuzu {
namespace storage {

class BMFileHandle;
class BufferManager;

// Keeps the state information of a page in a file.
class PageState {
    static constexpr uint64_t DIRTY_MASK = 0x0080000000000000;
    static constexpr uint64_t STATE_MASK = 0xFF00000000000000;
    static constexpr uint64_t VERSION_MASK = 0x00FFFFFFFFFFFFFF;
    static constexpr uint64_t NUM_BITS_TO_SHIFT_FOR_STATE = 56;

public:
    static constexpr uint64_t UNLOCKED = 0;
    static constexpr uint64_t LOCKED = 1;
    static constexpr uint64_t MARKED = 2;
    static constexpr uint64_t EVICTED = 3;

    PageState() {
        stateAndVersion.store(EVICTED << NUM_BITS_TO_SHIFT_FOR_STATE, std::memory_order_release);
    }

    inline uint64_t getState() { return getState(stateAndVersion.load()); }
    inline static uint64_t getState(uint64_t stateAndVersion) {
        return (stateAndVersion & STATE_MASK) >> NUM_BITS_TO_SHIFT_FOR_STATE;
    }
    inline static uint64_t getVersion(uint64_t stateAndVersion) {
        return stateAndVersion & VERSION_MASK;
    }
    inline static uint64_t updateStateWithSameVersion(
        uint64_t oldStateAndVersion, uint64_t newState) {
        return ((oldStateAndVersion << 8) >> 8) | (newState << NUM_BITS_TO_SHIFT_FOR_STATE);
    }
    inline static uint64_t updateStateAndIncrementVersion(
        uint64_t oldStateAndVersion, uint64_t newState) {
        return (((oldStateAndVersion << 8) >> 8) + 1) | (newState << NUM_BITS_TO_SHIFT_FOR_STATE);
    }
    inline void spinLock(uint64_t oldStateAndVersion) {
        while (true) {
            if (tryLock(oldStateAndVersion)) {
                return;
            }
        }
    }
    inline bool tryLock(uint64_t oldStateAndVersion) {
        return stateAndVersion.compare_exchange_strong(
            oldStateAndVersion, updateStateWithSameVersion(oldStateAndVersion, LOCKED));
    }
    inline void unlock() {
        // TODO(Keenan / Guodong): Track down this rare bug and re-enable the assert. Ref #2289.
        // KU_ASSERT(getState(stateAndVersion.load()) == LOCKED);
        stateAndVersion.store(updateStateAndIncrementVersion(stateAndVersion.load(), UNLOCKED),
            std::memory_order_release);
    }
    // Change page state from Mark to Unlocked.
    inline bool tryClearMark(uint64_t oldStateAndVersion) {
        KU_ASSERT(getState(oldStateAndVersion) == MARKED);
        return stateAndVersion.compare_exchange_strong(
            oldStateAndVersion, updateStateWithSameVersion(oldStateAndVersion, UNLOCKED));
    }
    inline bool tryMark(uint64_t oldStateAndVersion) {
        return stateAndVersion.compare_exchange_strong(
            oldStateAndVersion, updateStateWithSameVersion(oldStateAndVersion, MARKED));
    }

    inline void setDirty() {
        KU_ASSERT(getState(stateAndVersion.load()) == LOCKED);
        stateAndVersion |= DIRTY_MASK;
    }
    inline void clearDirty() {
        KU_ASSERT(getState(stateAndVersion.load()) == LOCKED);
        stateAndVersion &= ~DIRTY_MASK;
    }
    inline bool isDirty() const { return stateAndVersion & DIRTY_MASK; }
    uint64_t getStateAndVersion() const { return stateAndVersion.load(); }

    inline void resetToEvicted() {
        stateAndVersion.store(EVICTED << NUM_BITS_TO_SHIFT_FOR_STATE, std::memory_order_release);
    }

private:
    // Highest 1 bit is dirty bit, and the rest are page state and version bits.
    // In the rest bits, the lowest 1 byte is state, and the rest are version.
    std::atomic<uint64_t> stateAndVersion;
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

    ~BMFileHandle();

    // This function assumes the page is already LOCKED.
    inline void setLockedPageDirty(common::page_idx_t pageIdx) {
        KU_ASSERT(pageIdx < numPages);
        pageStates[pageIdx]->setDirty();
    }

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
        KU_ASSERT(pageIdx < numPages && pageStates[pageIdx]);
        return pageStates[pageIdx].get();
    }
    inline common::frame_idx_t getFrameIdx(common::page_idx_t pageIdx) {
        KU_ASSERT(pageIdx < pageCapacity);
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
