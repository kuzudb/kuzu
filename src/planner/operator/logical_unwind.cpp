#include "planner/operator/logical_unwind.h"

#include "planner/operator/factorization/flatten_resolver.h"

using namespace kuzu::binder;
using namespace kuzu::common;

namespace kuzu {
namespace planner {

f_group_pos_set LogicalUnwind::getGroupsPosToFlatten() {
    auto childSchema = children[0]->getSchema();
    auto dependentGroupsPos = childSchema->getDependentGroupsPos(inExpr);
    return factorization::FlattenAll::getGroupsPosToFlatten(dependentGroupsPos, childSchema);
}

void LogicalUnwind::computeFactorizedSchema() {
    copyChildSchema(0);
    auto groupPos = schema->createGroup();
    schema->insertToGroupAndScope(outExpr, groupPos);
    if (hasIDExpr()) {
        schema->insertToGroupAndScope(idExpr, groupPos);
    }
}

void LogicalUnwind::computeFlatSchema() {
    copyChildSchema(0);
    schema->insertToGroupAndScope(outExpr, 0);
    if (hasIDExpr()) {
        schema->insertToGroupAndScope(idExpr, 0);
    }
}

} // namespace planner
} // namespace kuzu
