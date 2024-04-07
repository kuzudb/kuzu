#include "planner/operator/persistent/logical_merge.h"

#include "binder/expression/node_expression.h"
#include "common/cast.h"
#include "planner/operator/factorization/flatten_resolver.h"

using namespace kuzu::binder;
using namespace kuzu::common;

namespace kuzu {
namespace planner {

void LogicalMerge::computeFactorizedSchema() {
    copyChildSchema(0);
    for (auto& info : insertNodeInfos) {
        // Predicate iri is not matched but needs to be inserted.
        auto node = ku_dynamic_cast<Expression*, NodeExpression*>(info.pattern.get());
        if (!schema->isExpressionInScope(*node->getInternalID())) {
            auto groupPos = schema->createGroup();
            schema->setGroupAsSingleState(groupPos);
            schema->insertToGroupAndScope(node->getInternalID(), groupPos);
        }
    }
}

void LogicalMerge::computeFlatSchema() {
    copyChildSchema(0);
    for (auto& info : insertNodeInfos) {
        auto node = ku_dynamic_cast<Expression*, NodeExpression*>(info.pattern.get());
        schema->insertToGroupAndScopeMayRepeat(node->getInternalID(), 0);
    }
}

f_group_pos_set LogicalMerge::getGroupsPosToFlatten() {
    auto childSchema = children[0]->getSchema();
    return factorization::FlattenAll::getGroupsPosToFlatten(childSchema->getGroupsPosInScope(),
        childSchema);
}

} // namespace planner
} // namespace kuzu
