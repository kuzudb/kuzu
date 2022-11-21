#include "planner/logical_plan/logical_operator/logical_projection.h"

namespace kuzu {
namespace planner {

string LogicalProjection::getExpressionsForPrinting() const {
    auto result = string();
    for (auto& expression : expressions) {
        result += expression->getUniqueName() + ", ";
    }
    return result;
}

} // namespace planner
} // namespace kuzu
