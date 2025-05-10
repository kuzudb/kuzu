#include "planner/operator/logical_table_function_call.h"
#include "processor/plan_mapper.h"

using namespace kuzu::planner;
using namespace kuzu::common;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapTableFunctionCall(
    const LogicalOperator* logicalOperator) {
    auto& call = logicalOperator->constCast<LogicalTableFunctionCall>();
    auto getPhysicalPlanFunc = call.getTableFunc().getPhysicalPlanFunc;
    KU_ASSERT(getPhysicalPlanFunc);
    auto res = getPhysicalPlanFunc(this, logicalOperator);
    logicalOpToPhysicalOpMap.insert({logicalOperator, res.get()});
    auto tableFuncOp = logicalOpToPhysicalOpMap[logicalOperator];
    for (auto i = 0u; i < call.getNumChildren(); ++i) {
        tableFuncOp->addChild(mapOperator(call.getChild(i).get()));
    }
    return res;
}

} // namespace processor
} // namespace kuzu
