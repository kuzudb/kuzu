#include "include/logical_result_collector.h"

namespace graphflow {
namespace planner {

string LogicalResultCollector::getExpressionsForPrinting() const {
    auto result = string();
    for (auto& expression : expressionsToCollect) {
        result += expression->getUniqueName() + ",";
    }
    return result;
}

} // namespace planner
} // namespace graphflow
