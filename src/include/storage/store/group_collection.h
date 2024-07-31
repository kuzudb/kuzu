#pragma once

#include <vector>

#include "common/serializer/deserializer.h"
#include "common/serializer/serializer.h"
#include "common/types/types.h"
#include "common/uniq_lock.h"

namespace kuzu {
namespace storage {

template<class T>
class GroupCollection {
public:
    GroupCollection() {}

    common::UniqLock lock() { return common::UniqLock{mtx}; }

    void loadGroups(common::Deserializer& deSer) {
        lock();
        deSer.deserializeVectorOfPtrs<T>(groups);
    }
    void serializeGroups(common::Serializer& ser) {
        lock();
        ser.serializeVectorOfPtrs<T>(groups);
    }

    void appendGroup(const common::UniqLock& lock, std::unique_ptr<T> group) {
        KU_ASSERT(group);
        KU_ASSERT(lock.isLocked());
        KU_UNUSED(lock);
        groups.push_back(std::move(group));
    }
    T* getGroup(const common::UniqLock& lock, common::idx_t groupIdx) {
        KU_ASSERT(lock.isLocked());
        KU_UNUSED(lock);
        if (groupIdx >= groups.size()) {
            return nullptr;
        }
        return groups[groupIdx].get();
    }
    T* getGroupNoLock(common::idx_t groupIdx) {
        if (groupIdx >= groups.size()) {
            return nullptr;
        }
        return groups[groupIdx].get();
    }
    void replaceGroup(const common::UniqLock& lock, common::idx_t groupIdx,
        std::unique_ptr<T> group) {
        KU_ASSERT(group);
        KU_ASSERT(lock.isLocked());
        KU_UNUSED(lock);
        if (groupIdx >= groups.size()) {
            groups.resize(groupIdx + 1);
        }
        groups[groupIdx] = std::move(group);
    }

    void resize(const common::UniqLock& lock, common::idx_t newSize) {
        KU_ASSERT(lock.isLocked());
        KU_UNUSED(lock);
        if (newSize <= groups.size()) {
            return;
        }
        groups.resize(newSize);
    }

    bool isEmpty(const common::UniqLock& lock) {
        KU_ASSERT(lock.isLocked());
        KU_UNUSED(lock);
        return groups.empty();
    }
    common::idx_t getNumGroups(const common::UniqLock& lock) const {
        KU_ASSERT(lock.isLocked());
        KU_UNUSED(lock);
        return groups.size();
    }

    const std::vector<std::unique_ptr<T>>& getAllGroups(const common::UniqLock& lock) {
        KU_ASSERT(lock.isLocked());
        KU_UNUSED(lock);
        return groups;
    }
    std::unique_ptr<T> moveGroup(const common::UniqLock& lock, common::idx_t groupIdx) {
        KU_ASSERT(groupIdx < groups.size());
        KU_ASSERT(lock.isLocked());
        KU_UNUSED(lock);
        return std::move(groups[groupIdx]);
    }
    T* getFirstGroup(const common::UniqLock& lock) {
        KU_ASSERT(lock.isLocked());
        KU_UNUSED(lock);
        if (groups.empty()) {
            return nullptr;
        }
        return groups.front().get();
    }
    T* getFirstGroupNoLock() {
        if (groups.empty()) {
            return nullptr;
        }
        return groups.front().get();
    }
    T* getLastGroup(const common::UniqLock& lock) {
        KU_ASSERT(lock.isLocked());
        KU_UNUSED(lock);
        if (groups.empty()) {
            return nullptr;
        }
        return groups.back().get();
    }

    void clear(const common::UniqLock& lock) {
        KU_ASSERT(lock.isLocked());
        KU_UNUSED(lock);
        groups.clear();
    }

private:
    std::mutex mtx;
    std::vector<std::unique_ptr<T>> groups;
};

} // namespace storage
} // namespace kuzu
