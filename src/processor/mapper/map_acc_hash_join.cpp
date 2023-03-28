#include "planner/logical_plan/logical_operator/logical_semi_masker.h"
#include "processor/mapper/plan_mapper.h"
#include "processor/operator/scan_node_id.h"
#include "processor/operator/semi_masker.h"
#include "processor/operator/table_scan/factorized_table_scan.h"

using namespace kuzu::planner;

namespace kuzu {
namespace processor {

static FactorizedTableScan* getTableScanForAccHashJoin(PhysicalOperator* probe) {
    auto op = probe->getChild(0);
    while (op->getOperatorType() == PhysicalOperatorType::FLATTEN) {
        op = op->getChild(0);
    }
    assert(op->getOperatorType() == PhysicalOperatorType::FACTORIZED_TABLE_SCAN);
    return (FactorizedTableScan*)op;
}

void PlanMapper::mapAccHashJoin(kuzu::processor::PhysicalOperator* probe) {
    auto tableScan = getTableScanForAccHashJoin(probe);
    auto resultCollector = tableScan->moveUnaryChild();
    probe->addChild(std::move(resultCollector));
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapLogicalSemiMaskerToPhysical(
    LogicalOperator* logicalOperator) {
    auto logicalSemiMasker = (LogicalSemiMasker*)logicalOperator;
    auto inSchema = logicalSemiMasker->getChild(0)->getSchema();
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->getChild(0));
    std::vector<ScanNodeIDSharedState*> scanStates;
    for (auto& op : logicalSemiMasker->getScanNodes()) {
        auto physicalScanNode = (ScanNodeID*)logicalOpToPhysicalOpMap.at(op);
        scanStates.push_back(physicalScanNode->getSharedState());
    }
    auto keyDataPos = DataPos(inSchema->getExpressionPos(*logicalSemiMasker->getNodeID()));
    if (logicalSemiMasker->isMultiLabel()) {
        return std::make_unique<MultiTableSemiMasker>(keyDataPos, std::move(scanStates),
            std::move(prevOperator), getOperatorID(),
            logicalSemiMasker->getExpressionsForPrinting());
    } else {
        return std::make_unique<SingleTableSemiMasker>(keyDataPos, std::move(scanStates),
            std::move(prevOperator), getOperatorID(),
            logicalSemiMasker->getExpressionsForPrinting());
    }
}

} // namespace processor
} // namespace kuzu
