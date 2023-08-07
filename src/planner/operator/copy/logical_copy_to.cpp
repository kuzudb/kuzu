#include "planner/logical_plan/copy/logical_copy_to.h"

#include "planner/logical_plan/factorization/flatten_resolver.h"

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
    f_group_pos_set dependentGroupsPos;
    for (auto& expression : childSchema->getExpressionsInScope()) {
        for (auto& grouPos : childSchema->getDependentGroupsPos(expression)) {
            dependentGroupsPos.insert(grouPos);
        }
    }
    return factorization::FlattenAll::getGroupsPosToFlatten(dependentGroupsPos, childSchema);
}

} // namespace planner
} // namespace kuzu
