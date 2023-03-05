#include "planner/logical_plan/logical_operator/logical_ftable_scan.h"

namespace kuzu {
namespace planner {

void LogicalFTableScan::computeFactorizedSchema() {
    createEmptySchema();
    for (auto [prevPos, expressions] : populateGroupPosToExpressionsMap()) {
        auto newPos = schema->createGroup();
        schema->insertToGroupAndScope(expressions, newPos);
        if (schemaToScanFrom->getGroup(prevPos)->isFlat()) {
            schema->setGroupAsSingleState(newPos);
        }
    }
}

void LogicalFTableScan::computeFlatSchema() {
    createEmptySchema();
    schema->createGroup();
    for (auto& expression : expressionsToScan) {
        schema->insertToGroupAndScope(expression, 0);
    }
}

std::unordered_map<uint32_t, binder::expression_vector>
LogicalFTableScan::populateGroupPosToExpressionsMap() {
    std::unordered_map<uint32_t, binder::expression_vector> groupPosToExpressionsMap;
    for (auto& expression : expressionsToScan) {
        auto groupPos = schemaToScanFrom->getGroupPos(expression->getUniqueName());
        if (!groupPosToExpressionsMap.contains(groupPos)) {
            groupPosToExpressionsMap.insert({groupPos, binder::expression_vector{}});
        }
        groupPosToExpressionsMap.at(groupPos).push_back(expression);
    }
    return groupPosToExpressionsMap;
}

} // namespace planner
} // namespace kuzu
