#include "binder/query/updating_clause/bound_create_clause.h"

namespace kuzu {
namespace binder {

std::vector<expression_pair> BoundCreateClause::getAllSetItems() const {
    std::vector<expression_pair> result;
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

std::unique_ptr<BoundUpdatingClause> BoundCreateClause::copy() {
    auto result = std::make_unique<BoundCreateClause>();
    for (auto& createNode : createNodes) {
        result->addCreateNode(createNode->copy());
    }
    for (auto& createRel : createRels) {
        result->addCreateRel(createRel->copy());
    }
    return result;
}

} // namespace binder
} // namespace kuzu
