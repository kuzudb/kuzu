#include "binder/query/reading_clause/bound_gds_call.h"
#include "planner/operator/logical_gds_call.h"
#include "planner/planner.h"

using namespace kuzu::binder;

namespace kuzu {
namespace planner {

std::shared_ptr<LogicalOperator> Planner::getGDSCall(const BoundGDSCallInfo& info) {
    auto bindData = info.func.gds->getBindData();
    std::vector<std::shared_ptr<LogicalOperator>> nodeMaskRoots;
    for (auto& nodeInfo : bindData->graphEntry.nodeInfos) {
        if (nodeInfo.predicate == nullptr) {
            continue;
        }
        auto p = planNodeSemiMask(SemiMaskTargetType::GDS_GRAPH_NODE,
            nodeInfo.nodeOrRel->constCast<NodeExpression>(), nodeInfo.predicate);
        nodeMaskRoots.push_back(p.getLastOperator());
    }
    auto op = std::make_shared<LogicalGDSCall>(info.copy(), std::move(nodeMaskRoots));
    op->computeFactorizedSchema();
    return op;
}

} // namespace planner
} // namespace kuzu
