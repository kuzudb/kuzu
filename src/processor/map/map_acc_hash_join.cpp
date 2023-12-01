#include "processor/operator/table_scan/factorized_table_scan.h"
#include "processor/plan_mapper.h"

using namespace kuzu::planner;

namespace kuzu {
namespace processor {

static FactorizedTableScan* getTableScanForAccHashJoin(PhysicalOperator* probe) {
    auto op = probe->getChild(0);
    while (op->getOperatorType() == PhysicalOperatorType::FLATTEN) {
        op = op->getChild(0);
    }
    KU_ASSERT(op->getOperatorType() == PhysicalOperatorType::FACTORIZED_TABLE_SCAN);
    return (FactorizedTableScan*)op;
}

void PlanMapper::mapSIPJoin(kuzu::processor::PhysicalOperator* probe) {
    auto tableScan = getTableScanForAccHashJoin(probe);
    auto resultCollector = tableScan->moveUnaryChild();
    probe->addChild(std::move(resultCollector));
}

} // namespace processor
} // namespace kuzu
