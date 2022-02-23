#include "include/logical_projection.h"

namespace graphflow {
namespace planner {

string LogicalProjection::getExpressionsForPrinting() const {
    auto result = string();
    for (auto& expression : expressionsToProject) {
        result += expression->getUniqueName() + ", ";
    }
    return result;
}

} // namespace planner
} // namespace graphflow
