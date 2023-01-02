#include "planner/logical_plan/logical_operator/logical_ftable_scan.h"

namespace kuzu {
namespace planner {

void LogicalFTableScan::computeSchema() {
    createEmptySchema();
    for (auto [prevPos, expressions] : populateGroupPosToExpressionsMap()) {
        auto newPos = schema->createGroup();
        schema->insertToGroupAndScope(expressions, newPos);
        if (schemaToScanFrom->getGroup(prevPos)->isFlat()) {
            schema->setGroupAsSingleState(newPos);
        }
    }
}

unordered_map<uint32_t, expression_vector> LogicalFTableScan::populateGroupPosToExpressionsMap() {
    unordered_map<uint32_t, expression_vector> groupPosToExpressionsMap;
    for (auto& expression : expressionsToScan) {
        auto groupPos = schemaToScanFrom->getGroupPos(expression->getUniqueName());
        if (!groupPosToExpressionsMap.contains(groupPos)) {
            groupPosToExpressionsMap.insert({groupPos, expression_vector{}});
        }
        groupPosToExpressionsMap.at(groupPos).push_back(expression);
    }
    return groupPosToExpressionsMap;
}

} // namespace planner
} // namespace kuzu
