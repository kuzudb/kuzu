#include "planner/operator/persistent/logical_copy_to.h"

#include "planner/operator/factorization/flatten_resolver.h"

namespace kuzu {
namespace planner {

void LogicalCopyTo::computeFactorizedSchema() {
    copyChildSchema(0);
}

void LogicalCopyTo::computeFlatSchema() {
    copyChildSchema(0);
}

f_group_pos_set LogicalCopyTo::getGroupsPosToFlatten() {
    auto childSchema = children[0]->getSchema();
    auto dependentGroupsPos = childSchema->getGroupsPosInScope();
    return factorization::FlattenAllButOne::getGroupsPosToFlatten(dependentGroupsPos, childSchema);
}

} // namespace planner
} // namespace kuzu
