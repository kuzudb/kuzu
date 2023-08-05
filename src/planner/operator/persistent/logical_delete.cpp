#include "planner/logical_plan/persistent/logical_delete.h"

#include "planner/logical_plan/factorization/flatten_resolver.h"

namespace kuzu {
namespace planner {

std::vector<std::unique_ptr<LogicalDeleteNodeInfo>> LogicalDeleteNodeInfo::copy(
    const std::vector<std::unique_ptr<LogicalDeleteNodeInfo>>& infos) {
    std::vector<std::unique_ptr<LogicalDeleteNodeInfo>> infosCopy;
    infosCopy.reserve(infos.size());
    for (auto& info : infos) {
        infosCopy.push_back(info->copy());
    }
    return infosCopy;
}

std::string LogicalDeleteNode::getExpressionsForPrinting() const {
    std::string result;
    for (auto& info : infos) {
        result += info->node->toString() + ",";
    }
    return result;
}

f_group_pos_set LogicalDeleteNode::getGroupsPosToFlatten(uint32_t idx) {
    // TODO(Xiyang): See how we can optimize this to not flatten when deleting single label nodes.
    f_group_pos_set result;
    auto node = infos[idx]->node;
    auto childSchema = children[0]->getSchema();
    result.insert(childSchema->getGroupPos(*node->getInternalIDProperty()));
    return factorization::FlattenAll::getGroupsPosToFlatten(result, childSchema);
}

std::string LogicalDeleteRel::getExpressionsForPrinting() const {
    std::string result;
    for (auto& rel : rels) {
        result += rel->toString() + ",";
    }
    return result;
}

f_group_pos_set LogicalDeleteRel::getGroupsPosToFlatten(uint32_t idx) {
    f_group_pos_set result;
    auto rel = rels[idx];
    auto childSchema = children[0]->getSchema();
    result.insert(childSchema->getGroupPos(*rel->getSrcNode()->getInternalIDProperty()));
    result.insert(childSchema->getGroupPos(*rel->getDstNode()->getInternalIDProperty()));
    return factorization::FlattenAll::getGroupsPosToFlatten(result, childSchema);
}

} // namespace planner
} // namespace kuzu
