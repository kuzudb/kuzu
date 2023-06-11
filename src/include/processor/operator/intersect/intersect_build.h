#pragma once

#include "intersect_hash_table.h"
#include "processor/operator/hash_join/hash_join_build.h"

namespace kuzu {
namespace processor {

class IntersectSharedState : public HashJoinSharedState {
public:
    explicit IntersectSharedState(std::unique_ptr<IntersectHashTable> hashtable)
        : HashJoinSharedState{std::move(hashtable)} {}
};

class IntersectBuild : public HashJoinBuild {
public:
    IntersectBuild(std::unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        std::shared_ptr<IntersectSharedState> sharedState, std::unique_ptr<HashJoinBuildInfo> info,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : HashJoinBuild{std::move(resultSetDescriptor), PhysicalOperatorType::INTERSECT_BUILD,
              std::move(sharedState), std::move(info), std::move(child), id, paramsString} {}

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return make_unique<IntersectBuild>(resultSetDescriptor->copy(),
            common::ku_reinterpret_pointer_cast<HashJoinSharedState, IntersectSharedState>(
                sharedState),
            info->copy(), children[0]->clone(), id, paramsString);
    }

protected:
    inline void initLocalHashTable(storage::MemoryManager& memoryManager) override {
        hashTable = make_unique<IntersectHashTable>(memoryManager, info->getTableSchema()->copy());
    }
};

} // namespace processor
} // namespace kuzu
