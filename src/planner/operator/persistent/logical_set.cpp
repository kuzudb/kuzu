#include "planner/operator/persistent/logical_set.h"

#include "binder/expression/expression_util.h"
#include "binder/expression/rel_expression.h"
#include "planner/operator/factorization/flatten_resolver.h"

using namespace kuzu::binder;
using namespace kuzu::common;

namespace kuzu {
namespace planner {

void LogicalSetProperty::computeFactorizedSchema() {
    copyChildSchema(0);
}

void LogicalSetProperty::computeFlatSchema() {
    copyChildSchema(0);
}

f_group_pos_set LogicalSetProperty::getGroupsPosToFlatten(uint32_t idx) const {
    f_group_pos_set result;
    auto childSchema = children[0]->getSchema();
    auto& info = infos[idx];
    switch (getTableType()) {
    case TableType::NODE: {
        auto node = info.pattern->constPtrCast<NodeExpression>();
        result.insert(childSchema->getGroupPos(*node->getInternalID()));
    } break;
    case TableType::REL: {
        auto rel = info.pattern->constPtrCast<RelExpression>();
        result.insert(childSchema->getGroupPos(*rel->getSrcNode()->getInternalID()));
        result.insert(childSchema->getGroupPos(*rel->getDstNode()->getInternalID()));
    } break;
    default:
        KU_UNREACHABLE;
    }
    for (auto& groupPos : childSchema->getDependentGroupsPos(info.setItem.second)) {
        result.insert(groupPos);
    }
    return factorization::FlattenAll::getGroupsPosToFlatten(result, childSchema);
}

std::string LogicalSetProperty::getExpressionsForPrinting() const {
    std::string result = ExpressionUtil::toString(infos[0].setItem);
    for (auto i = 1u; i < infos.size(); ++i) {
        result += ExpressionUtil::toString(infos[i].setItem);
    }
    return result;
}

common::TableType LogicalSetProperty::getTableType() const {
    KU_ASSERT(!infos.empty());
    return infos[0].tableType;
}

} // namespace planner
} // namespace kuzu
