#pragma once

#include <memory>
#include <vector>

#include "common/exception/exception.h"
#include "common/uniq_lock.h"

namespace kuzu::common {
class CheckpointException : public Exception {
public:
    explicit CheckpointException(std::exception& e) : Exception(e.what()) {}
    void addLock(common::UniqLock lock) {
        locks.emplace_back(std::make_shared<common::UniqLock>(std::move(lock)));
    }
    std::vector<const common::UniqLock*> getLocks() const {
        std::vector<const common::UniqLock*> ret;
        for (const auto& lock : locks) {
            ret.push_back(lock.get());
        }
        return ret;
    }

private:
    std::vector<std::shared_ptr<common::UniqLock>> locks;
};
} // namespace kuzu::common
