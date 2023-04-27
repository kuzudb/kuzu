#pragma once

#include "processor/operator/mask.h"
#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

// Multiple maskers can point to the same SemiMask, thus we associate each masker with an idx
// to indicate the execution sequence of its pipeline. Also, the maskerIdx is used as a flag to
// indicate if a value in the mask is masked or not, as each masker will increment the selected
// value in the mask by 1. More details are described in NodeTableSemiMask.
using mask_with_idx = std::pair<NodeSemiMask*, uint8_t>;

class BaseSemiMasker : public PhysicalOperator {
protected:
    BaseSemiMasker(const DataPos& keyDataPos,
        std::unordered_map<common::table_id_t, std::vector<mask_with_idx>> masksPerTable,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::SEMI_MASKER, std::move(child), id, paramsString},
          keyDataPos{keyDataPos}, masksPerTable{std::move(masksPerTable)} {}

    void initGlobalStateInternal(ExecutionContext* context) override;

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

protected:
    DataPos keyDataPos;
    std::unordered_map<common::table_id_t, std::vector<mask_with_idx>> masksPerTable;
    std::shared_ptr<common::ValueVector> keyValueVector;
};

class SingleTableSemiMasker : public BaseSemiMasker {
public:
    SingleTableSemiMasker(const DataPos& keyDataPos,
        std::unordered_map<common::table_id_t, std::vector<mask_with_idx>> masksPerTable,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : BaseSemiMasker{keyDataPos, std::move(masksPerTable), std::move(child), id, paramsString} {
    }

    bool getNextTuplesInternal(ExecutionContext* context) override;

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<SingleTableSemiMasker>(
            keyDataPos, masksPerTable, children[0]->clone(), id, paramsString);
    }
};

class MultiTableSemiMasker : public BaseSemiMasker {
public:
    MultiTableSemiMasker(const DataPos& keyDataPos,
        std::unordered_map<common::table_id_t, std::vector<mask_with_idx>> masksPerTable,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : BaseSemiMasker{keyDataPos, std::move(masksPerTable), std::move(child), id, paramsString} {
    }

    bool getNextTuplesInternal(ExecutionContext* context) override;

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<MultiTableSemiMasker>(
            keyDataPos, masksPerTable, children[0]->clone(), id, paramsString);
    }
};

} // namespace processor
} // namespace kuzu
