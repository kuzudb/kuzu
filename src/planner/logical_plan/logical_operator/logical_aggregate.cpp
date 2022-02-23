#include "include/logical_aggregate.h"

namespace graphflow {
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
} // namespace graphflow
