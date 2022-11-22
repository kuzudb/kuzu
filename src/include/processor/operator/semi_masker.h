#pragma once

#include "processor/operator/physical_operator.h"
#include "processor/operator/scan_node_id.h"

namespace kuzu {
namespace processor {

class SemiMasker : public PhysicalOperator {

public:
    SemiMasker(const DataPos& keyDataPos, unique_ptr<PhysicalOperator> child, uint32_t id,
        const string& paramsString)
        : PhysicalOperator{std::move(child), id, paramsString},
          keyDataPos{keyDataPos}, maskerIdx{0}, scanNodeIDSharedState{nullptr} {}

    SemiMasker(const SemiMasker& other)
        : PhysicalOperator{other.children[0]->clone(), other.id, other.paramsString},
          keyDataPos{other.keyDataPos}, maskerIdx{other.maskerIdx},
          scanNodeIDSharedState{other.scanNodeIDSharedState} {}

    inline void setSharedState(ScanNodeIDSharedState* sharedState) {
        scanNodeIDSharedState = sharedState;
        maskerIdx = scanNodeIDSharedState->getNumMaskers();
        assert(maskerIdx < UINT8_MAX);
        scanNodeIDSharedState->incrementNumMaskers();
    }

    inline PhysicalOperatorType getOperatorType() override { return SEMI_MASKER; }

    shared_ptr<ResultSet> init(ExecutionContext* context) override;

    bool getNextTuples() override;

    inline unique_ptr<PhysicalOperator> clone() override { return make_unique<SemiMasker>(*this); }

private:
    DataPos keyDataPos;
    // Multiple maskers can point to the same scanNodeID, thus we associate each masker with an idx
    // to indicate the execution sequence of its pipeline. Also, the maskerIdx is used as a flag to
    // indicate if a value in the mask is masked or not, as each masker will increment the selected
    // value in the mask by 1. More details are described in ScanNodeIDSemiMask.
    uint8_t maskerIdx;
    shared_ptr<ValueVector> keyValueVector;
    ScanNodeIDSharedState* scanNodeIDSharedState;
};
} // namespace processor
} // namespace kuzu
