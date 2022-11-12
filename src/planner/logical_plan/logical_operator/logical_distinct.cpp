#include "include/logical_distinct.h"

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
