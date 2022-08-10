#pragma once

#include "src/processor/include/physical_plan/operator/physical_operator.h"
#include "src/processor/include/physical_plan/operator/scan_node_id.h"

namespace graphflow {
namespace processor {

class SemiMasker : public PhysicalOperator {

public:
    SemiMasker(const DataPos& keyDataPos, ScanNodeIDSharedState* scanNodeIDSharedState,
        unique_ptr<PhysicalOperator> child, uint32_t id, const string& paramsString)
        : PhysicalOperator{std::move(child), id, paramsString}, keyDataPos{keyDataPos},
          scanNodeIDSharedState{scanNodeIDSharedState} {}

    inline PhysicalOperatorType getOperatorType() override { return SEMI_MASKER; }

    shared_ptr<ResultSet> init(ExecutionContext* context) override;

    bool getNextTuples() override;

    inline unique_ptr<PhysicalOperator> clone() override {
        auto clonedSemiMasker = make_unique<SemiMasker>(
            keyDataPos, scanNodeIDSharedState, children[0]->clone(), id, paramsString);
    }

private:
    DataPos keyDataPos;
    shared_ptr<ValueVector> keyValueVector;
    ScanNodeIDSharedState* scanNodeIDSharedState;
};
} // namespace processor
} // namespace graphflow
