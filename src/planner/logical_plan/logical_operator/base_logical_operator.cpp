#include "include/base_logical_operator.h"

namespace graphflow {
namespace planner {

LogicalOperator::LogicalOperator(shared_ptr<LogicalOperator> child) {
    children.push_back(move(child));
}

LogicalOperator::LogicalOperator(
    shared_ptr<LogicalOperator> left, shared_ptr<LogicalOperator> right) {
    children.push_back(move(left));
    children.push_back(move(right));
}

bool LogicalOperator::descendantsContainType(
    const unordered_set<LogicalOperatorType>& types) const {
    if (types.contains(getLogicalOperatorType())) {
        return true;
    }
    for (auto& child : children) {
        if (child->descendantsContainType(types)) {
            return true;
        }
    }
    return false;
}

string LogicalOperator::toString(uint64_t depth) const {
    string result = string(depth * 4, ' ');
    result += LogicalOperatorTypeNames[getLogicalOperatorType()] + "[" +
              getExpressionsForPrinting() + "]";
    if (children.size() == 1) {
        result += "\n" + children[0]->toString(depth);
    } else if (children.size() == 2) {
        result += "\nLEFT:\n" + children[0]->toString(depth + 1);
        result += "\nRIGHT:\n" + children[1]->toString(depth + 1);
    }
    return result;
}

} // namespace planner
} // namespace graphflow
