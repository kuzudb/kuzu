#include "planner/logical_plan/logical_operator/logical_aggregate.h"

namespace kuzu {
namespace planner {

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
