#pragma once

#include "processor/operator/physical_operator.h"
#include "processor/operator/scan_node_id.h"

namespace kuzu {
namespace processor {

// Multiple maskers can point to the same SemiMask, thus we associate each masker with an idx
// to indicate the execution sequence of its pipeline. Also, the maskerIdx is used as a flag to
// indicate if a value in the mask is masked or not, as each masker will increment the selected
// value in the mask by 1. More details are described in NodeTableSemiMask.
using mask_and_idx_pair = std::pair<NodeTableSemiMask*, uint8_t>;

class BaseSemiMasker : public PhysicalOperator {
protected:
    BaseSemiMasker(const DataPos& keyDataPos, std::vector<ScanNodeIDSharedState*> scanStates,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::SEMI_MASKER, std::move(child), id, paramsString},
          keyDataPos{keyDataPos}, scanStates{std::move(scanStates)} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

protected:
    DataPos keyDataPos;
    std::vector<ScanNodeIDSharedState*> scanStates;
    std::shared_ptr<common::ValueVector> keyValueVector;
};

class SingleTableSemiMasker : public BaseSemiMasker {
public:
    SingleTableSemiMasker(const DataPos& keyDataPos, std::vector<ScanNodeIDSharedState*> scanStates,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : BaseSemiMasker{keyDataPos, std::move(scanStates), std::move(child), id, paramsString} {}

    void initGlobalStateInternal(kuzu::processor::ExecutionContext* context) override;

    bool getNextTuplesInternal(ExecutionContext* context) override;

    inline std::unique_ptr<PhysicalOperator> clone() override {
        auto result = std::make_unique<SingleTableSemiMasker>(
            keyDataPos, scanStates, children[0]->clone(), id, paramsString);
        result->maskPerScan = maskPerScan;
        return result;
    }

private:
    std::vector<mask_and_idx_pair> maskPerScan;
};

class MultiTableSemiMasker : public BaseSemiMasker {
public:
    MultiTableSemiMasker(const DataPos& keyDataPos, std::vector<ScanNodeIDSharedState*> scanStates,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : BaseSemiMasker{keyDataPos, std::move(scanStates), std::move(child), id, paramsString} {}

    void initGlobalStateInternal(kuzu::processor::ExecutionContext* context) override;

    bool getNextTuplesInternal(ExecutionContext* context) override;

    inline std::unique_ptr<PhysicalOperator> clone() override {
        auto result = std::make_unique<MultiTableSemiMasker>(
            keyDataPos, scanStates, children[0]->clone(), id, paramsString);
        result->maskerPerLabelPerScan = maskerPerLabelPerScan;
        return result;
    }

private:
    std::vector<std::unordered_map<common::table_id_t, mask_and_idx_pair>> maskerPerLabelPerScan;
};

} // namespace processor
} // namespace kuzu
