#pragma once

#include "intersect_hash_table.h"
#include "processor/operator/hash_join/hash_join_build.h"

namespace kuzu {
namespace processor {

class IntersectSharedState : public HashJoinSharedState {
public:
    IntersectSharedState() = default;

    void initEmptyHashTable(MemoryManager& memoryManager, uint64_t numKeyColumns,
        unique_ptr<FactorizedTableSchema> tableSchema) override;
};

class IntersectBuild : public HashJoinBuild {
public:
    IntersectBuild(unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        shared_ptr<IntersectSharedState> sharedState, const BuildDataInfo& buildDataInfo,
        unique_ptr<PhysicalOperator> child, uint32_t id, const string& paramsString)
        : HashJoinBuild{std::move(resultSetDescriptor), PhysicalOperatorType::INTERSECT_BUILD,
              std::move(sharedState), buildDataInfo, std::move(child), id, paramsString} {}

    inline unique_ptr<PhysicalOperator> clone() override {
        return make_unique<IntersectBuild>(resultSetDescriptor->copy(),
            ku_reinterpret_pointer_cast<HashJoinSharedState, IntersectSharedState>(sharedState),
            buildDataInfo, children[0]->clone(), id, paramsString);
    }

protected:
    void initLocalHashTable(
        MemoryManager& memoryManager, unique_ptr<FactorizedTableSchema> tableSchema) override;
};

} // namespace processor
} // namespace kuzu
