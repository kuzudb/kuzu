#pragma once

#include "intersect_hash_table.h"

#include "src/processor/operator/hash_join/include/hash_join_build.h"

namespace graphflow {
namespace processor {

class IntersectSharedState : public HashJoinSharedState {
public:
    explicit IntersectSharedState(vector<DataType> payloadDataTypes)
        : HashJoinSharedState{move(payloadDataTypes)} {}

    void initEmptyHashTableIfNecessary(MemoryManager& memoryManager, uint64_t numKeyColumns,
        unique_ptr<FactorizedTableSchema> tableSchema) override;
};

class IntersectBuild : public HashJoinBuild {
public:
    IntersectBuild(shared_ptr<IntersectSharedState> sharedState, const BuildDataInfo& buildDataInfo,
        unique_ptr<PhysicalOperator> child, uint32_t id, const string& paramsString)
        : HashJoinBuild{move(sharedState), buildDataInfo, move(child), id, paramsString} {}

    inline PhysicalOperatorType getOperatorType() override { return INTERSECT_BUILD; }

    inline unique_ptr<PhysicalOperator> clone() override {
        return make_unique<IntersectBuild>(
            reinterpret_pointer_cast<IntersectSharedState, HashJoinSharedState>(sharedState),
            buildDataInfo, children[0]->clone(), id, paramsString);
    }

protected:
    void initHashTable(
        MemoryManager& memoryManager, unique_ptr<FactorizedTableSchema> tableSchema) override;
};

} // namespace processor
} // namespace graphflow
