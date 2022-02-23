#include "include/logical_union.h"

namespace graphflow {
namespace planner {

string LogicalUnion::getExpressionsForPrinting() const {
    auto result = string();
    for (auto& expression : expressionsToUnion) {
        result += expression->getUniqueName() + ", ";
    }
    return result;
}

} // namespace planner
} // namespace graphflow
