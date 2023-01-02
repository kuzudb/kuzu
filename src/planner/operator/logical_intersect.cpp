#include "planner/logical_plan/logical_operator/logical_intersect.h"

namespace kuzu {
namespace planner {

void LogicalIntersect::computeSchema() {
    auto probeSchema = children[0]->getSchema();
    schema = probeSchema->copy();
    // Write intersect node and rels into a new group regardless of whether rel is n-n.
    auto outGroupPos = schema->createGroup();
    schema->insertToGroupAndScope(intersectNode->getInternalIDProperty(), outGroupPos);
    for (auto i = 1; i < children.size(); ++i) {
        auto buildSchema = children[i]->getSchema();
        auto buildInfo = buildInfos[i - 1].get();
        // Write rel properties into output group.
        for (auto& expression : buildSchema->getExpressionsInScope()) {
            if (expression->getUniqueName() == intersectNode->getInternalIDPropertyName() ||
                expression->getUniqueName() == buildInfo->key->getInternalIDPropertyName()) {
                continue;
            }
            schema->insertToGroupAndScope(expression, outGroupPos);
        }
    }
}

unique_ptr<LogicalOperator> LogicalIntersect::copy() {
    vector<shared_ptr<LogicalOperator>> buildChildren;
    vector<unique_ptr<LogicalIntersectBuildInfo>> buildInfos_;
    for (auto i = 1u; i < children.size(); ++i) {
        buildChildren.push_back(children[i]->copy());
        buildInfos_.push_back(buildInfos[i - 1]->copy());
    }
    auto result = make_unique<LogicalIntersect>(
        intersectNode, children[0]->copy(), std::move(buildChildren), std::move(buildInfos_));
    return result;
}

} // namespace planner
} // namespace kuzu
