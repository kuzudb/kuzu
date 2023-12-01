#include "planner/operator/logical_unwind.h"

#include "planner/operator/factorization/flatten_resolver.h"

namespace kuzu {
namespace planner {

f_group_pos_set LogicalUnwind::getGroupsPosToFlatten() {
    auto childSchema = children[0]->getSchema();
    auto dependentGroupsPos = childSchema->getDependentGroupsPos(expression);
    return factorization::FlattenAll::getGroupsPosToFlatten(dependentGroupsPos, childSchema);
}

void LogicalUnwind::computeFactorizedSchema() {
    copyChildSchema(0);
    auto groupPos = schema->createGroup();
    schema->insertToGroupAndScope(aliasExpression, groupPos);
}

void LogicalUnwind::computeFlatSchema() {
    copyChildSchema(0);
    schema->insertToGroupAndScope(aliasExpression, 0);
}

} // namespace planner
} // namespace kuzu
