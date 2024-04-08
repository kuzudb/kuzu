#include "binder/expression/expression_util.h"
#include "planner/operator/factorization/flatten_resolver.h"
#include "planner/operator/factorization/sink_util.h"
#include "planner/operator/logical_mark_accmulate.h"

using namespace kuzu::binder;

namespace kuzu {
namespace planner {

void LogicalMarkAccumulate::computeFactorizedSchema() {
    createEmptySchema();
    auto childSchema = children[0]->getSchema();
    SinkOperatorUtil::recomputeSchema(*childSchema, childSchema->getExpressionsInScope(), *schema);
    f_group_pos groupPos;
    if (!keys.empty()) {
        groupPos = schema->getGroupPos(*keys[0]);
    } else {
        groupPos = schema->createGroup();
    }
    schema->insertToGroupAndScope(mark, groupPos);
}

void LogicalMarkAccumulate::computeFlatSchema() {
    copyChildSchema(0);
    schema->insertToGroupAndScope(mark, 0);
}

f_group_pos_set LogicalMarkAccumulate::getGroupsPosToFlatten() const {
    auto childSchema = children[0]->getSchema();
    f_group_pos_set dependentGroupsPos = childSchema->getGroupsPosInScope();
    // TODO(Xiyang/Ziyi): we don't need to flatten all. But hash aggregate seems to not allow
    // flat key with unFlat payload.
    return factorization::FlattenAll::getGroupsPosToFlatten(dependentGroupsPos, childSchema);
}

expression_vector LogicalMarkAccumulate::getPayloads() const {
    auto exprs = children[0]->getSchema()->getExpressionsInScope();
    return ExpressionUtil::excludeExpressions(exprs, keys);
}

} // namespace planner
} // namespace kuzu
