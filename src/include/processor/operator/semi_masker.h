#pragma once

#include "processor/operator/physical_operator.h"
#include "processor/operator/scan_node_id.h"

namespace kuzu {
namespace processor {

class SemiMasker : public PhysicalOperator {

public:
    SemiMasker(const DataPos& keyDataPos, std::unique_ptr<PhysicalOperator> child, uint32_t id,
        const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::SEMI_MASKER, std::move(child), id, paramsString},
          keyDataPos{keyDataPos}, maskerIdx{0}, scanTableNodeIDSharedState{nullptr} {}

    SemiMasker(const SemiMasker& other)
        : PhysicalOperator{PhysicalOperatorType::SEMI_MASKER, other.children[0]->clone(), other.id,
              other.paramsString},
          keyDataPos{other.keyDataPos}, maskerIdx{other.maskerIdx},
          scanTableNodeIDSharedState{other.scanTableNodeIDSharedState} {}

    // This function is used in the plan mapper to configure the shared state between the SemiMasker
    // and ScanNodeID.
    void setSharedState(ScanTableNodeIDSharedState* sharedState);

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool getNextTuplesInternal() override;

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<SemiMasker>(*this);
    }

private:
    inline void initGlobalStateInternal(ExecutionContext* context) override {
        scanTableNodeIDSharedState->initSemiMask(context->transaction);
    }

private:
    DataPos keyDataPos;
    // Multiple maskers can point to the same scanNodeID, thus we associate each masker with an idx
    // to indicate the execution sequence of its pipeline. Also, the maskerIdx is used as a flag to
    // indicate if a value in the mask is masked or not, as each masker will increment the selected
    // value in the mask by 1. More details are described in ScanNodeIDSemiMask.
    uint8_t maskerIdx;
    std::shared_ptr<common::ValueVector> keyValueVector;
    ScanTableNodeIDSharedState* scanTableNodeIDSharedState;
};
} // namespace processor
} // namespace kuzu
