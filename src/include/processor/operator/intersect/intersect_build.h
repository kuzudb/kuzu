#pragma once

#include "intersect_hash_table.h"
#include "processor/operator/hash_join/hash_join_build.h"

namespace kuzu {
namespace processor {

class IntersectSharedState : public HashJoinSharedState {
public:
    IntersectSharedState() = default;

    void initEmptyHashTable(storage::MemoryManager& memoryManager, uint64_t numKeyColumns,
        std::unique_ptr<FactorizedTableSchema> tableSchema) override;
};

class IntersectBuild : public HashJoinBuild {
public:
    IntersectBuild(std::unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        std::shared_ptr<IntersectSharedState> sharedState, const BuildDataInfo& buildDataInfo,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : HashJoinBuild{std::move(resultSetDescriptor), PhysicalOperatorType::INTERSECT_BUILD,
              std::move(sharedState), buildDataInfo, std::move(child), id, paramsString} {}

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return make_unique<IntersectBuild>(resultSetDescriptor->copy(),
            common::ku_reinterpret_pointer_cast<HashJoinSharedState, IntersectSharedState>(
                sharedState),
            buildDataInfo, children[0]->clone(), id, paramsString);
    }

protected:
    void initLocalHashTable(storage::MemoryManager& memoryManager,
        std::unique_ptr<FactorizedTableSchema> tableSchema) override;
};

} // namespace processor
} // namespace kuzu
