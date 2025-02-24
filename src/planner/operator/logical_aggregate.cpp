#include "planner/operator/logical_aggregate.h"

#include "binder/expression/expression_util.h"
#include "planner/operator/factorization/flatten_resolver.h"

namespace kuzu {
namespace planner {

std::string LogicalAggregatePrintInfo::toString() const {
    std::string result = "";
    result += "Group By: ";
    result += binder::ExpressionUtil::toString(keys);
    result += ", Aggregates: ";
    result += binder::ExpressionUtil::toString(aggregates);
    return result;
}

void LogicalAggregate::computeFactorizedSchema() {
    createEmptySchema();
    auto groupPos = schema->createGroup();
    insertAllExpressionsToGroupAndScope(groupPos);
}

void LogicalAggregate::computeFlatSchema() {
    createEmptySchema();
    schema->createGroup();
    insertAllExpressionsToGroupAndScope(0 /* groupPos */);
}

f_group_pos_set LogicalAggregate::getGroupsPosToFlattenForGroupBy() {
    return FlattenAllButOne::getGroupsPosToFlatten(getAllKeys(), *children[0]->getSchema());
}

f_group_pos_set LogicalAggregate::getGroupsPosToFlattenForAggregate() {
    return f_group_pos_set{};
}

std::string LogicalAggregate::getExpressionsForPrinting() const {
    std::string result = "Group By [";
    for (auto& expression : keys) {
        result += expression->toString() + ", ";
    }
    for (auto& expression : dependentKeys) {
        result += expression->toString() + ", ";
    }
    result += "], Aggregate [";
    for (auto& expression : aggregates) {
        result += expression->toString() + ", ";
    }
    result += "]";
    return result;
}

void LogicalAggregate::insertAllExpressionsToGroupAndScope(f_group_pos groupPos) {
    for (auto& expression : keys) {
        schema->insertToGroupAndScopeMayRepeat(expression, groupPos);
    }
    for (auto& expression : dependentKeys) {
        schema->insertToGroupAndScopeMayRepeat(expression, groupPos);
    }
    for (auto& expression : aggregates) {
        schema->insertToGroupAndScopeMayRepeat(expression, groupPos);
    }
}

} // namespace planner
} // namespace kuzu
