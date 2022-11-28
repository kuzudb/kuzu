#pragma once

#include "result_collector.h"

namespace kuzu {
namespace processor {

class CrossProduct : public PhysicalOperator {
public:
    CrossProduct(shared_ptr<FTableSharedState> sharedState,
        vector<pair<DataPos, DataType>> outVecPosAndTypePairs, vector<uint32_t> colIndicesToScan,
        vector<uint64_t> flatDataChunkPositions, unique_ptr<PhysicalOperator> probeChild,
        unique_ptr<PhysicalOperator> buildChild, uint32_t id, const string& paramsString)
        : PhysicalOperator{std::move(probeChild), std::move(buildChild), id, paramsString},
          sharedState{std::move(sharedState)}, outVecPosAndTypePairs{std::move(
                                                   outVecPosAndTypePairs)},
          colIndicesToScan{std::move(colIndicesToScan)}, flatDataChunkPositions{
                                                             std::move(flatDataChunkPositions)} {}

    // Clone only.
    CrossProduct(shared_ptr<FTableSharedState> sharedState,
        vector<pair<DataPos, DataType>> outVecPosAndTypePairs, vector<uint32_t> colIndicesToScan,
        vector<uint64_t> flatDataChunkPositions, unique_ptr<PhysicalOperator> child, uint32_t id,
        const string& paramsString)
        : PhysicalOperator{std::move(child), id, paramsString}, sharedState{std::move(sharedState)},
          outVecPosAndTypePairs{std::move(outVecPosAndTypePairs)},
          colIndicesToScan{std::move(colIndicesToScan)}, flatDataChunkPositions{
                                                             std::move(flatDataChunkPositions)} {}

    PhysicalOperatorType getOperatorType() override { return PhysicalOperatorType::CROSS_PRODUCT; }

    shared_ptr<ResultSet> init(kuzu::processor::ExecutionContext* context) override;

    bool getNextTuplesInternal() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<CrossProduct>(sharedState, outVecPosAndTypePairs, colIndicesToScan,
            flatDataChunkPositions, children[0]->clone(), id, paramsString);
    }

private:
    shared_ptr<FTableSharedState> sharedState;
    vector<pair<DataPos, DataType>> outVecPosAndTypePairs;
    vector<uint32_t> colIndicesToScan;
    vector<uint64_t> flatDataChunkPositions;

    uint64_t startIdx = 0u;
    vector<shared_ptr<ValueVector>> vectorsToScan;
};

} // namespace processor
} // namespace kuzu
