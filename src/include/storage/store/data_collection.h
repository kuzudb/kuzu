#pragma once

#include <memory>
#include <vector>

#incl
#include "common/serializer/deserializer.h"
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

    void appendGroup(common::UniqLock& lock, std::unique_ptr<T> group) {
        KU_ASSERT(group);
        KU_ASSERT(lock.isLocked());
        groups.push_back(std::move(group));
    }
    T* getGroup(common::UniqLock& lock, common::row_idx_t rowIdx) {
        KU_ASSERT(lock.isLocked());
        KU_ASSERT(rowIdx < groups.size() * common::StorageConstants::NODE_GROUP_SIZE);
    }
    bool isEmpty(common::UniqLock& lock) {
        KU_ASSERT(lock.isLocked());
        return groups.empty();
    }
    common::idx_t getNumGroups(common::UniqLock& lock) const {
        KU_ASSERT(lock.isLocked());
        return groups.size();
    }

    const std::vector<std::unique_ptr<T>>& getAllGroups(common::UniqLock& lock) {
        KU_ASSERT(lock.isLocked());
        return groups;
    }
    T* getLastGroup(common::UniqLock& lock) {
        KU_ASSERT(lock.isLocked());
        KU_ASSERT(!groups.empty());
        return groups.back().get();
    }

private:
    std::mutex mtx;
    std::vector<std::unique_ptr<T>> groups;
};

} // namespace storage
} // namespace kuzu
