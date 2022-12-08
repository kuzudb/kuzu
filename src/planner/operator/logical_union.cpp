#include "planner/logical_plan/logical_operator/logical_union.h"

namespace kuzu {
namespace planner {

string LogicalUnion::getExpressionsForPrinting() const {
    auto result = string();
    for (auto& expression : expressionsToUnion) {
        result += expression->getUniqueName() + ", ";
    }
    return result;
}

} // namespace planner
} // namespace kuzu
