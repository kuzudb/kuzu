#include "planner/operator/logical_distinct.h"

#include "planner/operator/factorization/flatten_resolver.h"

namespace kuzu {
namespace planner {

void LogicalDistinct::computeFactorizedSchema() {
    createEmptySchema();
    auto groupPos = schema->createGroup();
    for (auto& expression : getAllDistinctExpressions()) {
        schema->insertToGroupAndScope(expression, groupPos);
    }
}

void LogicalDistinct::computeFlatSchema() {
    createEmptySchema();
    schema->createGroup();
    for (auto& expression : getAllDistinctExpressions()) {
        schema->insertToGroupAndScope(expression, 0);
    }
}

f_group_pos_set LogicalDistinct::getGroupsPosToFlatten() {
    f_group_pos_set dependentGroupsPos;
    auto childSchema = children[0]->getSchema();
    for (auto& expression : getAllDistinctExpressions()) {
        for (auto groupPos : childSchema->getDependentGroupsPos(expression)) {
            dependentGroupsPos.insert(groupPos);
        }
    }
    return factorization::FlattenAll::getGroupsPosToFlatten(dependentGroupsPos, childSchema);
}

std::string LogicalDistinct::getExpressionsForPrinting() const {
    std::string result;
    for (auto& expression : getAllDistinctExpressions()) {
        result += expression->getUniqueName() + ", ";
    }
    return result;
}

} // namespace planner
} // namespace kuzu
