#include "planner/logical_plan/persistent/logical_create.h"

#include "planner/logical_plan/factorization/flatten_resolver.h"

namespace kuzu {
namespace planner {

std::vector<std::unique_ptr<LogicalCreateNodeInfo>> LogicalCreateNodeInfo::copy(
    const std::vector<std::unique_ptr<LogicalCreateNodeInfo>>& infos) {
    std::vector<std::unique_ptr<LogicalCreateNodeInfo>> infosCopy;
    infosCopy.reserve(infos.size());
    for (auto& info : infos) {
        infosCopy.push_back(info->copy());
    }
    return infosCopy;
}

std::vector<std::unique_ptr<LogicalCreateRelInfo>> LogicalCreateRelInfo::copy(
    const std::vector<std::unique_ptr<LogicalCreateRelInfo>>& infos) {
    std::vector<std::unique_ptr<LogicalCreateRelInfo>> infosCopy;
    infosCopy.reserve(infos.size());
    for (auto& info : infos) {
        infosCopy.push_back(info->copy());
    }
    return infosCopy;
}

void LogicalCreateNode::computeFactorizedSchema() {
    copyChildSchema(0);
    for (auto& info : infos) {
        auto groupPos = schema->createGroup();
        schema->setGroupAsSingleState(groupPos);
        for (auto& property : info->propertiesToReturn) {
            schema->insertToGroupAndScope(property, groupPos);
        }
        schema->insertToGroupAndScopeMayRepeat(info->node->getInternalIDProperty(), groupPos);
    }
}

void LogicalCreateNode::computeFlatSchema() {
    copyChildSchema(0);
    for (auto& info : infos) {
        for (auto& property : info->propertiesToReturn) {
            schema->insertToGroupAndScope(property, 0);
        }
        schema->insertToGroupAndScopeMayRepeat(info->node->getInternalIDProperty(), 0);
    }
}

std::string LogicalCreateNode::getExpressionsForPrinting() const {
    std::string result;
    for (auto& info : infos) {
        result += info->node->toString() + ",";
    }
    return result;
}

f_group_pos_set LogicalCreateNode::getGroupsPosToFlatten() {
    // Flatten all inputs. E.g. MATCH (a) CREATE (b). We need to create b for each tuple in the
    // match clause. This is to simplify operator implementation.
    auto childSchema = children[0]->getSchema();
    return factorization::FlattenAll::getGroupsPosToFlatten(
        childSchema->getGroupsPosInScope(), childSchema);
}

void LogicalCreateRel::computeFactorizedSchema() {
    copyChildSchema(0);
    for (auto& info : infos) {
        auto groupPos = schema->createGroup();
        schema->setGroupAsSingleState(groupPos);
        for (auto& property : info->propertiesToReturn) {
            schema->insertToGroupAndScope(property, groupPos);
        }
    }
}

void LogicalCreateRel::computeFlatSchema() {
    copyChildSchema(0);
    for (auto& info : infos) {
        for (auto& property : info->propertiesToReturn) {
            schema->insertToGroupAndScope(property, 0);
        }
    }
}

std::string LogicalCreateRel::getExpressionsForPrinting() const {
    std::string result;
    for (auto& info : infos) {
        result += info->rel->toString() + ",";
    }
    return result;
}

f_group_pos_set LogicalCreateRel::getGroupsPosToFlatten() {
    auto childSchema = children[0]->getSchema();
    return factorization::FlattenAll::getGroupsPosToFlatten(
        childSchema->getGroupsPosInScope(), childSchema);
}

} // namespace planner
} // namespace kuzu
