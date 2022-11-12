#include "include/bound_create_clause.h"

namespace kuzu {
namespace binder {

vector<expression_pair> BoundCreateClause::getAllSetItems() const {
    vector<expression_pair> result;
    for (auto& createNode : createNodes) {
        for (auto& setItem : createNode->getSetItems()) {
            result.push_back(setItem);
        }
    }
    for (auto& createRel : createRels) {
        for (auto& setItem : createRel->getSetItems()) {
            result.push_back(setItem);
        }
    }
    return result;
}

expression_vector BoundCreateClause::getPropertiesToRead() const {
    expression_vector result;
    for (auto& setItem : getAllSetItems()) {
        for (auto& property : setItem.second->getSubPropertyExpressions()) {
            result.push_back(property);
        }
    }
    return result;
}

unique_ptr<BoundUpdatingClause> BoundCreateClause::copy() {
    vector<unique_ptr<BoundCreateNode>> copiedCreateNodes;
    vector<unique_ptr<BoundCreateRel>> copiedCreateRels;
    for (auto& createNode : createNodes) {
        copiedCreateNodes.push_back(createNode->copy());
    }
    for (auto& createRel : createRels) {
        copiedCreateRels.push_back(createRel->copy());
    }
    return make_unique<BoundCreateClause>(
        std::move(copiedCreateNodes), std::move(copiedCreateRels));
}

} // namespace binder
} // namespace kuzu
