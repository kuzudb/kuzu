#include "planner/operator/logical_limit.h"

#include "planner/operator/factorization/flatten_resolver.h"

namespace kuzu {
namespace planner {

std::string LogicalLimit::getExpressionsForPrinting() const {
    std::string result;
    if (hasSkipNum()) {
        result += "SKIP " + std::to_string(skipNum) + "\n";
    }
    if (hasLimitNum()) {
        result += "LIMIT " + std::to_string(limitNum) + "\n";
    }
    return result;
}

f_group_pos_set LogicalLimit::getGroupsPosToFlatten() {
    auto childSchema = children[0]->getSchema();
    return factorization::FlattenAllButOne::getGroupsPosToFlatten(
        childSchema->getGroupsPosInScope(), childSchema);
}

f_group_pos LogicalLimit::getGroupPosToSelect() const {
    auto childSchema = children[0]->getSchema();
    auto groupsPosInScope = childSchema->getGroupsPosInScope();
    SchemaUtils::validateAtMostOneUnFlatGroup(groupsPosInScope, *childSchema);
    return SchemaUtils::getLeadingGroupPos(groupsPosInScope, *childSchema);
}

} // namespace planner
} // namespace kuzu
