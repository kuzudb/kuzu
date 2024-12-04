#pragma once

#include <vector>

#include "common/exception/exception.h"
#include "common/uniq_lock.h"

namespace kuzu::common {
class CheckpointException : public Exception {
public:
    explicit CheckpointException(std::exception& e) : Exception(e.what()) {}
    void addLock(common::UniqLock lock) { locks.emplace_back(std::move(lock)); }
    const std::vector<common::UniqLock>& getLocks() const { return locks; }

private:
    std::exception internalException;
    std::vector<common::UniqLock> locks;
};
} // namespace kuzu::common
