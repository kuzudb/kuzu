#include "processor/plan_mapper.h"

using namespace kuzu::planner;

namespace kuzu {
namespace processor {

static PhysicalOperator* getTableScan(PhysicalOperator* joinRoot) {
    auto op = joinRoot->getChild(0);
    while (op->getOperatorType() == PhysicalOperatorType::FLATTEN) {
        op = op->getChild(0);
    }
    KU_ASSERT(op->getOperatorType() == PhysicalOperatorType::TABLE_FUNCTION_CALL);
    return op;
}

void PlanMapper::mapSIPJoin(kuzu::processor::PhysicalOperator* joinRoot) {
    auto tableScan = getTableScan(joinRoot);
    auto resultCollector = tableScan->moveUnaryChild();
    joinRoot->addChild(std::move(resultCollector));
}

} // namespace processor
} // namespace kuzu
