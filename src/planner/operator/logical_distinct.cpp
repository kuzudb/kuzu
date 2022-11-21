#include "planner/logical_plan/logical_operator/logical_distinct.h"

namespace kuzu {
namespace planner {

string LogicalDistinct::getExpressionsForPrinting() const {
    string result;
    for (auto& expression : expressionsToDistinct) {
        result += expression->getUniqueName() + ", ";
    }
    return result;
}

} // namespace planner
} // namespace kuzu
