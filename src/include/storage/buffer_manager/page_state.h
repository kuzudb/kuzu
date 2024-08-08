#pragma once

#include <atomic>

#include "common/assert.h"

namespace kuzu {
namespace storage {

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

    uint64_t getState() const { return getState(stateAndVersion.load()); }
    static uint64_t getState(uint64_t stateAndVersion) {
        return (stateAndVersion & STATE_MASK) >> NUM_BITS_TO_SHIFT_FOR_STATE;
    }
    static uint64_t getVersion(uint64_t stateAndVersion) { return stateAndVersion & VERSION_MASK; }
    static uint64_t updateStateWithSameVersion(uint64_t oldStateAndVersion, uint64_t newState) {
        return ((oldStateAndVersion << 8) >> 8) | (newState << NUM_BITS_TO_SHIFT_FOR_STATE);
    }
    static uint64_t updateStateAndIncrementVersion(uint64_t oldStateAndVersion, uint64_t newState) {
        return (((oldStateAndVersion << 8) >> 8) + 1) | (newState << NUM_BITS_TO_SHIFT_FOR_STATE);
    }
    void spinLock(uint64_t oldStateAndVersion) {
        while (true) {
            if (tryLock(oldStateAndVersion)) {
                return;
            }
        }
    }
    bool tryLock(uint64_t oldStateAndVersion) {
        return stateAndVersion.compare_exchange_strong(oldStateAndVersion,
            updateStateWithSameVersion(oldStateAndVersion, LOCKED));
    }
    void unlock() {
        // TODO(Keenan / Guodong): Track down this rare bug and re-enable the assert. Ref #2289.
        // KU_ASSERT(getState(stateAndVersion.load()) == LOCKED);
        stateAndVersion.store(updateStateAndIncrementVersion(stateAndVersion.load(), UNLOCKED));
    }
    // Change page state from Mark to Unlocked.
    bool tryClearMark(uint64_t oldStateAndVersion) {
        KU_ASSERT(getState(oldStateAndVersion) == MARKED);
        return stateAndVersion.compare_exchange_strong(oldStateAndVersion,
            updateStateWithSameVersion(oldStateAndVersion, UNLOCKED));
    }
    bool tryMark(uint64_t oldStateAndVersion) {
        return stateAndVersion.compare_exchange_strong(oldStateAndVersion,
            updateStateWithSameVersion(oldStateAndVersion, MARKED));
    }

    void setDirty() {
        KU_ASSERT(getState(stateAndVersion.load()) == LOCKED);
        stateAndVersion |= DIRTY_MASK;
    }
    void clearDirty() {
        KU_ASSERT(getState(stateAndVersion.load()) == LOCKED);
        stateAndVersion &= ~DIRTY_MASK;
    }
    // Meant to be used when flushing in a single thread.
    // Should not be used if other threads are modifying the page state
    void clearDirtyWithoutLock() { stateAndVersion &= ~DIRTY_MASK; }
    bool isDirty() const { return stateAndVersion & DIRTY_MASK; }
    uint64_t getStateAndVersion() const { return stateAndVersion.load(); }

    void resetToEvicted() { stateAndVersion.store(EVICTED << NUM_BITS_TO_SHIFT_FOR_STATE); }

private:
    // Highest 1 bit is dirty bit, and the rest are page state and version bits.
    // In the rest bits, the lowest 1 byte is state, and the rest are version.
    std::atomic<uint64_t> stateAndVersion;
};

} // namespace storage
} // namespace kuzu
