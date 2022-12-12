#pragma once

#include "result_collector.h"

namespace kuzu {
namespace processor {

class CrossProduct : public PhysicalOperator {
public:
    CrossProduct(shared_ptr<FTableSharedState> sharedState, vector<DataPos> outVecPos,
        vector<uint32_t> colIndicesToScan, unique_ptr<PhysicalOperator> probeChild,
        unique_ptr<PhysicalOperator> buildChild, uint32_t id, const string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::CROSS_PRODUCT, std::move(probeChild),
              std::move(buildChild), id, paramsString},
          sharedState{std::move(sharedState)}, outVecPos{std::move(outVecPos)},
          colIndicesToScan{std::move(colIndicesToScan)} {}

    // Clone only.
    CrossProduct(shared_ptr<FTableSharedState> sharedState, vector<DataPos> outVecPos,
        vector<uint32_t> colIndicesToScan, unique_ptr<PhysicalOperator> child, uint32_t id,
        const string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::CROSS_PRODUCT, std::move(child), id, paramsString},
          sharedState{std::move(sharedState)}, outVecPos{std::move(outVecPos)},
          colIndicesToScan{std::move(colIndicesToScan)} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool getNextTuplesInternal() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<CrossProduct>(
            sharedState, outVecPos, colIndicesToScan, children[0]->clone(), id, paramsString);
    }

private:
    shared_ptr<FTableSharedState> sharedState;
    vector<DataPos> outVecPos;
    vector<uint32_t> colIndicesToScan;

    uint64_t startIdx = 0u;
    vector<shared_ptr<ValueVector>> vectorsToScan;
};

} // namespace processor
} // namespace kuzu
