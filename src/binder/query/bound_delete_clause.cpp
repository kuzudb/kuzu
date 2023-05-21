#include "binder/query/updating_clause/bound_delete_clause.h"

namespace kuzu {
namespace binder {

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
