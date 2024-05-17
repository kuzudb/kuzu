#include "binder/expression/node_rel_expression.h"

using namespace kuzu::common;

namespace kuzu {
namespace binder {

void NodeOrRelExpression::addTableIDs(const table_id_vector_t& tableIDsToAdd) {
    auto tableIDsSet = getTableIDsSet();
    for (auto tableID : tableIDsToAdd) {
        if (!tableIDsSet.contains(tableID)) {
            tableIDs.push_back(tableID);
        }
    }
}

common::table_id_t NodeOrRelExpression::getSingleTableID() const {
    KU_ASSERT(tableIDs.size() == 1);
    return tableIDs[0];
}

void NodeOrRelExpression::addPropertyExpression(const std::string& propertyName,
    std::unique_ptr<Expression> property) {
    KU_ASSERT(!propertyNameToIdx.contains(propertyName));
    propertyNameToIdx.insert({propertyName, propertyExprs.size()});
    propertyExprs.push_back(std::move(property));
}

std::shared_ptr<Expression> NodeOrRelExpression::getPropertyExpression(
    const std::string& propertyName) const {
    KU_ASSERT(propertyNameToIdx.contains(propertyName));
    return propertyExprs[propertyNameToIdx.at(propertyName)]->copy();
}

expression_vector NodeOrRelExpression::getPropertyExprs() const {
    expression_vector result;
    for (auto& expr : propertyExprs) {
        result.push_back(expr->copy());
    }
    return result;
}

} // namespace binder
} // namespace kuzu
