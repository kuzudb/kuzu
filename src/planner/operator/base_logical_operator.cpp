#include "planner/logical_plan/logical_operator/base_logical_operator.h"

namespace kuzu {
namespace planner {

LogicalOperator::LogicalOperator(shared_ptr<LogicalOperator> child) {
    children.push_back(move(child));
}

LogicalOperator::LogicalOperator(
    shared_ptr<LogicalOperator> left, shared_ptr<LogicalOperator> right) {
    children.push_back(move(left));
    children.push_back(move(right));
}

LogicalOperator::LogicalOperator(vector<shared_ptr<LogicalOperator>> children) {
    for (auto& child : children) {
        this->children.push_back(child);
    }
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
    auto padding = string(depth * 4, ' ');
    string result = padding;
    result += LogicalOperatorTypeNames[getLogicalOperatorType()] + "[" +
              getExpressionsForPrinting() + "]";
    if (children.size() == 1) {
        result += "\n" + children[0]->toString(depth);
    } else {
        for (auto& child : children) {
            result += "\n" + padding + "CHILD:\n" + child->toString(depth + 1);
        }
    }
    return result;
}

} // namespace planner
} // namespace kuzu
