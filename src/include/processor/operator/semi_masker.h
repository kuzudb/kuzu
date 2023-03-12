#pragma once

#include "processor/operator/physical_operator.h"
#include "processor/operator/scan_node_id.h"

namespace kuzu {
namespace processor {

class BaseSemiMasker : public PhysicalOperator {
protected:
    BaseSemiMasker(const DataPos& keyDataPos, ScanNodeIDSharedState* scanNodeIDSharedState,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::SEMI_MASKER, std::move(child), id, paramsString},
          keyDataPos{keyDataPos}, scanNodeIDSharedState{scanNodeIDSharedState} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

protected:
    DataPos keyDataPos;
    ScanNodeIDSharedState* scanNodeIDSharedState;
    std::shared_ptr<common::ValueVector> keyValueVector;
};

class SingleTableSemiMasker : public BaseSemiMasker {
public:
    SingleTableSemiMasker(const DataPos& keyDataPos, ScanNodeIDSharedState* scanNodeIDSharedState,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : BaseSemiMasker{keyDataPos, scanNodeIDSharedState, std::move(child), id, paramsString} {}

    void initGlobalStateInternal(kuzu::processor::ExecutionContext* context) override;

    bool getNextTuplesInternal() override;

    inline std::unique_ptr<PhysicalOperator> clone() override {
        auto result = std::make_unique<SingleTableSemiMasker>(
            keyDataPos, scanNodeIDSharedState, children[0]->clone(), id, paramsString);
        result->maskerIdxAndMask = maskerIdxAndMask;
        return result;
    }

private:
    // Multiple maskers can point to the same SemiMask, thus we associate each masker with an idx
    // to indicate the execution sequence of its pipeline. Also, the maskerIdx is used as a flag to
    // indicate if a value in the mask is masked or not, as each masker will increment the selected
    // value in the mask by 1. More details are described in NodeTableSemiMask.
    std::pair<uint8_t, NodeTableSemiMask*> maskerIdxAndMask;
};

class MultiTableSemiMasker : public BaseSemiMasker {
public:
    MultiTableSemiMasker(const DataPos& keyDataPos, ScanNodeIDSharedState* scanNodeIDSharedState,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : BaseSemiMasker{keyDataPos, scanNodeIDSharedState, std::move(child), id, paramsString} {}

    void initGlobalStateInternal(kuzu::processor::ExecutionContext* context) override;

    bool getNextTuplesInternal() override;

    inline std::unique_ptr<PhysicalOperator> clone() override {
        auto result = std::make_unique<MultiTableSemiMasker>(
            keyDataPos, scanNodeIDSharedState, children[0]->clone(), id, paramsString);
        result->maskerIdxAndMasks = maskerIdxAndMasks;
        return result;
    }

private:
    std::unordered_map<common::table_id_t, std::pair<uint8_t, NodeTableSemiMask*>>
        maskerIdxAndMasks;
};

} // namespace processor
} // namespace kuzu
