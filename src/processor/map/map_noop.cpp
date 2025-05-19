#include "processor/plan_mapper.h"

using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapNoop(const LogicalOperator* logicalOperator) {
    auto op =
        createEmptyFTableScan(FactorizedTable::EmptyTable(clientContext->getMemoryManager()), 1);
    for (auto child : logicalOperator->getChildren()) {
        op->addChild(mapOperator(child.get()));
    }
    return op;
}

} // namespace processor
} // namespace kuzu
