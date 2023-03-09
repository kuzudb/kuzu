#include "planner/logical_plan/logical_operator/logical_distinct.h"

#include "planner/logical_plan/logical_operator/flatten_resolver.h"

namespace kuzu {
namespace planner {

void LogicalDistinct::computeFactorizedSchema() {
    createEmptySchema();
    auto groupPos = schema->createGroup();
    for (auto& expression : expressionsToDistinct) {
        schema->insertToGroupAndScope(expression, groupPos);
    }
}

void LogicalDistinct::computeFlatSchema() {
    createEmptySchema();
    schema->createGroup();
    for (auto& expression : expressionsToDistinct) {
        schema->insertToGroupAndScope(expression, 0);
    }
}

f_group_pos_set LogicalDistinct::getGroupsPosToFlatten() {
    f_group_pos_set dependentGroupsPos;
    auto childSchema = children[0]->getSchema();
    for (auto& expression : expressionsToDistinct) {
        for (auto groupPos : childSchema->getDependentGroupsPos(expression)) {
            dependentGroupsPos.insert(groupPos);
        }
    }
    return factorization::FlattenAll::getGroupsPosToFlatten(dependentGroupsPos, childSchema);
}

std::string LogicalDistinct::getExpressionsForPrinting() const {
    std::string result;
    for (auto& expression : expressionsToDistinct) {
        result += expression->getUniqueName() + ", ";
    }
    return result;
}

} // namespace planner
} // namespace kuzu
