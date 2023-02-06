#include "planner/logical_plan/logical_operator/logical_union.h"

#include "planner/logical_plan/logical_operator/sink_util.h"

namespace kuzu {
namespace planner {

void LogicalUnion::computeSchema() {
    auto firstChildSchema = children[0]->getSchema();
    schema = std::make_unique<Schema>();
    SinkOperatorUtil::recomputeSchema(
        *firstChildSchema, firstChildSchema->getExpressionsInScope(), *schema);
}

std::unique_ptr<LogicalOperator> LogicalUnion::copy() {
    std::vector<std::shared_ptr<LogicalOperator>> copiedChildren;
    for (auto i = 0u; i < getNumChildren(); ++i) {
        copiedChildren.push_back(getChild(i)->copy());
    }
    return make_unique<LogicalUnion>(expressionsToUnion, std::move(copiedChildren));
}

} // namespace planner
} // namespace kuzu
