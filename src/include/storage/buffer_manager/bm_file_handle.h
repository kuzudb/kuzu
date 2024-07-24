#pragma once

#include <atomic>
#include <cmath>
#include <cstdint>

#include "common/concurrent_vector.h"
#include "common/constants.h"
#include "common/copy_constructors.h"
#include "common/types/types.h"
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

    PageState() { stateAndVersion.store(EVICTED << NUM_BITS_TO_SHIFT_FOR_STATE); }

    inline uint64_t getState() const { return getState(stateAndVersion.load()); }
    inline static uint64_t getState(uint64_t stateAndVersion) {
        return (stateAndVersion & STATE_MASK) >> NUM_BITS_TO_SHIFT_FOR_STATE;
    }
    inline static uint64_t getVersion(uint64_t stateAndVersion) {
        return stateAndVersion & VERSION_MASK;
    }
    inline static uint64_t updateStateWithSameVersion(uint64_t oldStateAndVersion,
        uint64_t newState) {
        return ((oldStateAndVersion << 8) >> 8) | (newState << NUM_BITS_TO_SHIFT_FOR_STATE);
    }
    inline static uint64_t updateStateAndIncrementVersion(uint64_t oldStateAndVersion,
        uint64_t newState) {
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
        return stateAndVersion.compare_exchange_strong(oldStateAndVersion,
            updateStateWithSameVersion(oldStateAndVersion, LOCKED));
    }
    inline void unlock() {
        // TODO(Keenan / Guodong): Track down this rare bug and re-enable the assert. Ref #2289.
        // KU_ASSERT(getState(stateAndVersion.load()) == LOCKED);
        stateAndVersion.store(updateStateAndIncrementVersion(stateAndVersion.load(), UNLOCKED));
    }
    // Change page state from Mark to Unlocked.
    inline bool tryClearMark(uint64_t oldStateAndVersion) {
        KU_ASSERT(getState(oldStateAndVersion) == MARKED);
        return stateAndVersion.compare_exchange_strong(oldStateAndVersion,
            updateStateWithSameVersion(oldStateAndVersion, UNLOCKED));
    }
    inline bool tryMark(uint64_t oldStateAndVersion) {
        return stateAndVersion.compare_exchange_strong(oldStateAndVersion,
            updateStateWithSameVersion(oldStateAndVersion, MARKED));
    }

    inline void setDirty() {
        KU_ASSERT(getState(stateAndVersion.load()) == LOCKED);
        stateAndVersion |= DIRTY_MASK;
    }
    inline void clearDirty() {
        KU_ASSERT(getState(stateAndVersion.load()) == LOCKED);
        stateAndVersion &= ~DIRTY_MASK;
    }
    // Meant to be used when flushing in a single thread.
    // Should not be used if other threads are modifying the page state
    inline void clearDirtyWithoutLock() { stateAndVersion &= ~DIRTY_MASK; }
    inline bool isDirty() const { return stateAndVersion & DIRTY_MASK; }
    uint64_t getStateAndVersion() const { return stateAndVersion.load(); }

    inline void resetToEvicted() { stateAndVersion.store(EVICTED << NUM_BITS_TO_SHIFT_FOR_STATE); }

private:
    // Highest 1 bit is dirty bit, and the rest are page state and version bits.
    // In the rest bits, the lowest 1 byte is state, and the rest are version.
    std::atomic<uint64_t> stateAndVersion;
};

// TODO(Guodong): Unify this class with FileHandle. We don't need to differentiate between the BM
// one and non-BM one anymore. BMFileHandle is a file handle that is backed by BufferManager. It
// holds the state of each page in the file. File Handle is the bridge between a data structure and
// the Buffer Manager that abstracts the file in which that data structure is stored.
class BMFileHandle final : public FileHandle {
    friend class BufferManager;

public:
    // This function assumes the page is already LOCKED.
    inline void setLockedPageDirty(common::page_idx_t pageIdx) {
        KU_ASSERT(pageIdx < numPages);
        pageStates[pageIdx].setDirty();
    }

    // This function is intended to be used after a fileInfo is created and we want the file
    // to have no pages and page locks. Should be called after ensuring that the buffer manager
    // does not hold any of the pages of the file.
    void resetToZeroPagesAndPageCapacity();
    void removePageIdxAndTruncateIfNecessary(common::page_idx_t pageIdx);

    common::file_idx_t getFileIndex() const { return fileIndex; }

private:
    BMFileHandle(const std::string& path, uint8_t flags, BufferManager* bm, uint32_t fileIndex,
        common::PageSizeClass pageSizeClass, common::VirtualFileSystem* vfs,
        main::ClientContext* context);
    // File handles are registered with the buffer manager and must not be moved or copied
    DELETE_COPY_AND_MOVE(BMFileHandle);
    inline PageState* getPageState(common::page_idx_t pageIdx) {
        KU_ASSERT(verifyPageIdx(pageIdx));
        return &pageStates[pageIdx];
    }
    inline common::frame_idx_t getFrameIdx(common::page_idx_t pageIdx) {
        KU_ASSERT(pageIdx < pageCapacity);
        return (frameGroupIdxes[pageIdx >> common::StorageConstants::PAGE_GROUP_SIZE_LOG2]
                   << common::StorageConstants::PAGE_GROUP_SIZE_LOG2) |
               (pageIdx & common::StorageConstants::PAGE_IDX_IN_GROUP_MASK);
    }
    inline common::PageSizeClass getPageSizeClass() const { return pageSizeClass; }

    common::page_idx_t addNewPageWithoutLock() override;
    void addNewPageGroupWithoutLock();
    inline common::page_group_idx_t getNumPageGroups() const {
        return ceil(static_cast<double>(numPages) / common::StorageConstants::PAGE_GROUP_SIZE);
    }

    bool verifyPageIdx(common::page_idx_t pageIdx) {
        std::unique_lock xLck{fhSharedMutex};
        return pageIdx < numPages;
    }

private:
    BufferManager* bm;
    common::PageSizeClass pageSizeClass;
    // With a page group size of 2^10 and an 256KB index size, the access cost increases
    // only with each 128GB added to the file
    common::ConcurrentVector<PageState, common::StorageConstants::PAGE_GROUP_SIZE,
        common::BufferPoolConstants::PAGE_256KB_SIZE / sizeof(void*)>
        pageStates;
    // Each file page group corresponds to a frame group in the VMRegion.
    // Just one frame group for each page group, so performance is less sensitive than pageStates
    // and left at the default which won't increase access cost for the frame groups until 16TB of
    // data has been written
    common::ConcurrentVector<common::page_group_idx_t> frameGroupIdxes;
    common::file_idx_t fileIndex;
};
} // namespace storage
} // namespace kuzu
