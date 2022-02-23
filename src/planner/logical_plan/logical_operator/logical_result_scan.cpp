#include "include/logical_result_scan.h"

namespace graphflow {
namespace planner {

string LogicalResultScan::getExpressionsForPrinting() const {
    string result;
    for (auto& expression : expressionsToScan) {
        result += expression->getUniqueName() + ", ";
    }
    return result;
}

} // namespace planner
} // namespace graphflow
