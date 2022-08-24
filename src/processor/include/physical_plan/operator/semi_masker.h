#pragma once

#include "src/processor/include/physical_plan/operator/physical_operator.h"
#include "src/processor/include/physical_plan/operator/scan_node_id.h"

namespace graphflow {
namespace processor {

class SemiMasker : public PhysicalOperator {

public:
    SemiMasker(string nodeID, const DataPos& keyDataPos, unique_ptr<PhysicalOperator> child,
        uint32_t id, const string& paramsString)
        : PhysicalOperator{std::move(child), id, paramsString}, nodeID{move(nodeID)},
          keyDataPos{keyDataPos} {}

    inline PhysicalOperatorType getOperatorType() override { return SEMI_MASKER; }

    inline void setSharedState(ScanNodeIDSharedState* state) { scanNodeIDSharedState = state; }

    shared_ptr<ResultSet> init(ExecutionContext* context) override;

    bool getNextTuples() override;

    inline unique_ptr<PhysicalOperator> clone() override {
        auto clonedSemiMasker =
            make_unique<SemiMasker>(nodeID, keyDataPos, children[0]->clone(), id, paramsString);
        clonedSemiMasker->setSharedState(scanNodeIDSharedState);
        return clonedSemiMasker;
    }

public:
    string nodeID;

private:
    DataPos keyDataPos;
    shared_ptr<ValueVector> keyValueVector;
    ScanNodeIDSharedState* scanNodeIDSharedState;
};
} // namespace processor
} // namespace graphflow
