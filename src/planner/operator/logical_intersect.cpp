#include "planner/logical_plan/logical_operator/logical_intersect.h"

namespace kuzu {
namespace planner {

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
