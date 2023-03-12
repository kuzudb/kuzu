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

void PlanMapper::mapASP(kuzu::processor::PhysicalOperator* probe) {
    auto tableScan = getTableScanForAccHashJoin(probe);
    auto resultCollector = tableScan->moveUnaryChild();
    probe->addChild(std::move(resultCollector));
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapLogicalSemiMaskerToPhysical(
    LogicalOperator* logicalOperator) {
    auto logicalSemiMasker = (LogicalSemiMasker*)logicalOperator;
    auto inSchema = logicalSemiMasker->getChild(0)->getSchema();
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->getChild(0));
    auto logicalScanNode = logicalSemiMasker->getScanNode();
    auto physicalScanNode = (ScanNodeID*)logicalOpToPhysicalOpMap.at(logicalScanNode);
    auto keyDataPos =
        DataPos(inSchema->getExpressionPos(*logicalScanNode->getNode()->getInternalIDProperty()));
    auto semiMasker = make_unique<SemiMasker>(keyDataPos, std::move(prevOperator), getOperatorID(),
        logicalSemiMasker->getExpressionsForPrinting());
    assert(physicalScanNode->getSharedState()->getNumTableStates() == 1);
    semiMasker->setSharedState(physicalScanNode->getSharedState()->getTableState(0));
    return semiMasker;
}

} // namespace processor
} // namespace kuzu
