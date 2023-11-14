#include "planner/operator/persistent/logical_insert.h"

#include "planner/operator/factorization/flatten_resolver.h"

namespace kuzu {
namespace planner {

std::vector<std::unique_ptr<LogicalInsertNodeInfo>> LogicalInsertNodeInfo::copy(
    const std::vector<std::unique_ptr<LogicalInsertNodeInfo>>& infos) {
    std::vector<std::unique_ptr<LogicalInsertNodeInfo>> infosCopy;
    infosCopy.reserve(infos.size());
    for (auto& info : infos) {
        infosCopy.push_back(info->copy());
    }
    return infosCopy;
}

std::vector<std::unique_ptr<LogicalInsertRelInfo>> LogicalInsertRelInfo::copy(
    const std::vector<std::unique_ptr<LogicalInsertRelInfo>>& infos) {
    std::vector<std::unique_ptr<LogicalInsertRelInfo>> infosCopy;
    infosCopy.reserve(infos.size());
    for (auto& info : infos) {
        infosCopy.push_back(info->copy());
    }
    return infosCopy;
}

void LogicalInsertNode::computeFactorizedSchema() {
    copyChildSchema(0);
    for (auto& info : infos) {
        auto groupPos = schema->createGroup();
        schema->setGroupAsSingleState(groupPos);
        for (auto& property : info->propertiesToReturn) {
            schema->insertToGroupAndScope(property, groupPos);
        }
        schema->insertToGroupAndScopeMayRepeat(info->node->getInternalID(), groupPos);
    }
}

void LogicalInsertNode::computeFlatSchema() {
    copyChildSchema(0);
    for (auto& info : infos) {
        for (auto& property : info->propertiesToReturn) {
            schema->insertToGroupAndScope(property, 0);
        }
        schema->insertToGroupAndScopeMayRepeat(info->node->getInternalID(), 0);
    }
}

std::string LogicalInsertNode::getExpressionsForPrinting() const {
    std::string result;
    for (auto& info : infos) {
        result += info->node->toString() + ",";
    }
    return result;
}

f_group_pos_set LogicalInsertNode::getGroupsPosToFlatten() {
    // Flatten all inputs. E.g. MATCH (a) CREATE (b). We need to create b for each tuple in the
    // match clause. This is to simplify operator implementation.
    auto childSchema = children[0]->getSchema();
    return factorization::FlattenAll::getGroupsPosToFlatten(
        childSchema->getGroupsPosInScope(), childSchema);
}

void LogicalInsertRel::computeFactorizedSchema() {
    copyChildSchema(0);
    for (auto& info : infos) {
        auto groupPos = schema->createGroup();
        schema->setGroupAsSingleState(groupPos);
        for (auto& property : info->propertiesToReturn) {
            schema->insertToGroupAndScope(property, groupPos);
        }
    }
}

void LogicalInsertRel::computeFlatSchema() {
    copyChildSchema(0);
    for (auto& info : infos) {
        for (auto& property : info->propertiesToReturn) {
            schema->insertToGroupAndScope(property, 0);
        }
    }
}

std::string LogicalInsertRel::getExpressionsForPrinting() const {
    std::string result;
    for (auto& info : infos) {
        result += info->rel->toString() + ",";
    }
    return result;
}

f_group_pos_set LogicalInsertRel::getGroupsPosToFlatten() {
    auto childSchema = children[0]->getSchema();
    return factorization::FlattenAll::getGroupsPosToFlatten(
        childSchema->getGroupsPosInScope(), childSchema);
}

} // namespace planner
} // namespace kuzu
