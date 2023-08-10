#include "planner/logical_plan/persistent/logical_set.h"

#include "binder/expression/rel_expression.h"
#include "planner/logical_plan/factorization/flatten_resolver.h"

using namespace kuzu::binder;

namespace kuzu {
namespace planner {

std::vector<std::unique_ptr<LogicalSetPropertyInfo>> LogicalSetPropertyInfo::copy(
    const std::vector<std::unique_ptr<LogicalSetPropertyInfo>>& infos) {
    std::vector<std::unique_ptr<LogicalSetPropertyInfo>> infosCopy;
    infosCopy.reserve(infos.size());
    for (auto& info : infos) {
        infosCopy.push_back(info->copy());
    }
    return infosCopy;
}

std::string LogicalSetNodeProperty::getExpressionsForPrinting() const {
    std::string result;
    for (auto& info : infos) {
        result += info->setItem.first->toString() + " = " + info->setItem.second->toString() + ",";
    }
    return result;
}

std::string LogicalSetRelProperty::getExpressionsForPrinting() const {
    std::string result;
    for (auto& info : infos) {
        result += info->setItem.first->toString() + " = " + info->setItem.second->toString() + ",";
    }
    return result;
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
