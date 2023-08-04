#include "planner/logical_plan/logical_operator/logical_set.h"

#include "binder/expression/rel_expression.h"
#include "planner/logical_plan/logical_operator/flatten_resolver.h"

using namespace kuzu::binder;

namespace kuzu {
namespace planner {

std::string LogicalSetNodeProperty::getExpressionsForPrinting() const {
    std::string result;
    for (auto& info : infos) {
        result += info->setItem.first->toString() + " = " + info->setItem.second->toString() + ",";
    }
    return result;
}

std::unique_ptr<LogicalOperator> LogicalSetNodeProperty::copy() {
    std::vector<std::unique_ptr<LogicalSetPropertyInfo>> infosCopy;
    for (auto& info : infos) {
        infosCopy.push_back(info->copy());
    }
    return std::make_unique<LogicalSetNodeProperty>(std::move(infosCopy), children[0]->copy());
}

std::string LogicalSetRelProperty::getExpressionsForPrinting() const {
    std::string result;
    for (auto& info : infos) {
        result += info->setItem.first->toString() + " = " + info->setItem.second->toString() + ",";
    }
    return result;
}

std::unique_ptr<LogicalOperator> LogicalSetRelProperty::copy() {
    std::vector<std::unique_ptr<LogicalSetPropertyInfo>> infosCopy;
    for (auto& info : infos) {
        infosCopy.push_back(info->copy());
    }
    return std::make_unique<LogicalSetRelProperty>(std::move(infosCopy), children[0]->copy());
}

f_group_pos_set LogicalSetRelProperty::getGroupsPosToFlatten(uint32_t idx) {
    f_group_pos_set result;
    auto rel = reinterpret_cast<RelExpression*>(infos[idx]->nodeOrRel.get());
    auto rhs = infos[idx]->setItem.second;
    auto childSchema = children[0]->getSchema();
    result.insert(childSchema->getGroupPos(*rel->getSrcNode()->getInternalIDProperty()));
    result.insert(childSchema->getGroupPos(*rel->getDstNode()->getInternalIDProperty()));
    for (auto groupPos : childSchema->getDependentGroupsPos(rhs)) {
        result.insert(groupPos);
    }
    return factorization::FlattenAll::getGroupsPosToFlatten(result, childSchema);
}

} // namespace planner
} // namespace kuzu
