#pragma once

#include <atomic>

namespace kuzu {
namespace common {
struct DatabaseLifeCycleManager {
    std::atomic<bool> isDatabaseClosed{false};
    // Set to true during WAL replay (recovery phase).
    // Extensions can check this to determine whether to load indexes synchronously (during
    // recovery) or asynchronously (normal operation). This prevents race conditions where
    // background index loading threads compete with WAL replay operations.
    std::atomic<bool> isRecoveryInProgress{false};
    void checkDatabaseClosedOrThrow() const;
};
} // namespace common
} // namespace kuzu
