#include "planner/logical_plan/logical_operator/logical_aggregate.h"

namespace kuzu {
namespace planner {

void LogicalAggregate::computeSchema() {
    createEmptySchema();
    auto groupPos = schema->createGroup();
    for (auto& expression : expressionsToGroupBy) {
        schema->insertToGroupAndScope(expression, groupPos);
    }
    for (auto& expression : expressionsToAggregate) {
        schema->insertToGroupAndScope(expression, groupPos);
    }
}

string LogicalAggregate::getExpressionsForPrinting() const {
    string result = "Group By [";
    for (auto& expression : expressionsToGroupBy) {
        result += expression->getUniqueName() + ", ";
    }
    result += "], Aggregate [";
    for (auto& expression : expressionsToAggregate) {
        result += expression->getUniqueName() + ", ";
    }
    result += "]";
    return result;
}

} // namespace planner
} // namespace kuzu
