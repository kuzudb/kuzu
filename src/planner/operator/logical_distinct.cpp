#include "planner/logical_plan/logical_operator/logical_distinct.h"

namespace kuzu {
namespace planner {

std::string LogicalDistinct::getExpressionsForPrinting() const {
    std::string result;
    for (auto& expression : expressionsToDistinct) {
        result += expression->getUniqueName() + ", ";
    }
    return result;
}

void LogicalDistinct::computeSchema() {
    createEmptySchema();
    auto groupPos = schema->createGroup();
    for (auto& expression : expressionsToDistinct) {
        schema->insertToGroupAndScope(expression, groupPos);
    }
}

} // namespace planner
} // namespace kuzu
