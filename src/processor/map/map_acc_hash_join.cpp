#include "processor/plan_mapper.h"

using namespace kuzu::planner;

namespace kuzu {
namespace processor {

static PhysicalOperator* getTableScan(PhysicalOperator* joinRoot) {
    auto op = joinRoot->getChild(0);
    while (op->getOperatorType() != PhysicalOperatorType::TABLE_FUNCTION_CALL) {
        KU_ASSERT(op->getNumChildren() != 0);
        op = op->getChild(0);
    }
    return op;
}

void PlanMapper::mapSIPJoin(kuzu::processor::PhysicalOperator* joinRoot) {
    auto tableScan = getTableScan(joinRoot);
    auto resultCollector = tableScan->moveUnaryChild();
    joinRoot->addChild(std::move(resultCollector));
}

} // namespace processor
} // namespace kuzu
