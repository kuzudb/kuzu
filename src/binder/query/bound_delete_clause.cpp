#include "binder/query/updating_clause/bound_delete_clause.h"

namespace kuzu {
namespace binder {

expression_vector BoundDeleteClause::getPropertiesToRead() const {
    expression_vector result;
    for (auto& deleteNode : deleteNodes) {
        result.push_back(deleteNode->getPrimaryKeyExpression());
    }
    for (auto& deleteRel : deleteRels) {
        if (deleteRel->hasInternalIDProperty()) {
            result.push_back(deleteRel->getInternalIDProperty());
        }
    }
    return result;
}

std::unique_ptr<BoundUpdatingClause> BoundDeleteClause::copy() {
    auto result = std::make_unique<BoundDeleteClause>();
    for (auto& deleteNode : deleteNodes) {
        result->addDeleteNode(deleteNode->copy());
    }
    for (auto& deleteRel : deleteRels) {
        result->addDeleteRel(deleteRel);
    }
    return result;
}

} // namespace binder
} // namespace kuzu
