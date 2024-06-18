#pragma once

#include "expression_evaluator/expression_evaluator.h"
#include "processor/operator/scan/scan_node_table.h"

namespace kuzu {
namespace processor {

struct PrimaryKeyScanSharedState {
    std::mutex mtx;

    common::idx_t numTables;
    common::idx_t cursor;

    explicit PrimaryKeyScanSharedState(common::idx_t numTables) : numTables{numTables}, cursor{0} {}

    common::idx_t getTableIdx();
};

class PrimaryKeyScanNodeTable : public ScanTable {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::PRIMARY_KEY_SCAN_NODE_TABLE;

public:
    PrimaryKeyScanNodeTable(ScanTableInfo info, std::vector<ScanNodeTableInfo> nodeInfos,
        std::unique_ptr<evaluator::ExpressionEvaluator> indexEvaluator,
        std::shared_ptr<PrimaryKeyScanSharedState> sharedState, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : ScanTable{type_, std::move(info), id, std::move(printInfo)},
          nodeInfos{std::move(nodeInfos)}, indexEvaluator{std::move(indexEvaluator)},
          sharedState{std::move(sharedState)} {}

    bool isSource() const override { return true; }

    void initLocalStateInternal(ResultSet*, ExecutionContext*) override;

    bool getNextTuplesInternal(ExecutionContext* context) override;

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<PrimaryKeyScanNodeTable>(info.copy(), copyVector(nodeInfos),
            indexEvaluator->clone(), sharedState, id, printInfo->copy());
    }

private:
    std::vector<ScanNodeTableInfo> nodeInfos;
    std::unique_ptr<evaluator::ExpressionEvaluator> indexEvaluator;
    std::shared_ptr<PrimaryKeyScanSharedState> sharedState;
};

} // namespace processor
} // namespace kuzu
