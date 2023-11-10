#include "planner/operator/persistent/logical_delete.h"

#include "planner/operator/factorization/flatten_resolver.h"

namespace kuzu {
namespace planner {

std::string LogicalDeleteNode::getExpressionsForPrinting() const {
    std::string result;
    for (auto& node : nodes) {
        result += node->toString() + ",";
    }
    return result;
}

f_group_pos_set LogicalDeleteNode::getGroupsPosToFlatten() {
    f_group_pos_set dependentGroupPos;
    auto childSchema = children[0]->getSchema();
    for (auto& node : nodes) {
        dependentGroupPos.insert(childSchema->getGroupPos(*node->getInternalID()));
    }
    return factorization::FlattenAll::getGroupsPosToFlatten(dependentGroupPos, childSchema);
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
    result.insert(childSchema->getGroupPos(*rel->getSrcNode()->getInternalID()));
    result.insert(childSchema->getGroupPos(*rel->getDstNode()->getInternalID()));
    return factorization::FlattenAll::getGroupsPosToFlatten(result, childSchema);
}

} // namespace planner
} // namespace kuzu
