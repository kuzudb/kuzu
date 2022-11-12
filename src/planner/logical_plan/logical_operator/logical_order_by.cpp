#include "include/logical_order_by.h"

namespace kuzu {
namespace planner {

string LogicalOrderBy::getExpressionsForPrinting() const {
    auto result = string();
    for (auto& expression : expressionsToOrderBy) {
        result += expression->getUniqueName() + ", ";
    }
    return result;
}

} // namespace planner
} // namespace kuzu
