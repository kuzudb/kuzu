#include "binder/expression/node_rel_expression.h"

#include "catalog/catalog_entry/table_catalog_entry.h"
#include "common/exception/runtime.h"

using namespace kuzu::catalog;
using namespace kuzu::common;

namespace kuzu {
namespace binder {

table_id_vector_t NodeOrRelExpression::getTableIDs() const {
    table_id_vector_t result;
    for (auto& entry : entries) {
        result.push_back(entry->getTableID());
    }
    return result;
}

table_id_set_t NodeOrRelExpression::getTableIDsSet() const {
    table_id_set_t result;
    for (auto& entry : entries) {
        result.insert(entry->getTableID());
    }
    return result;
}

void NodeOrRelExpression::addEntries(const std::vector<TableCatalogEntry*>& entries_) {
    auto tableIDsSet = getTableIDsSet();
    for (auto& entry : entries_) {
        if (!tableIDsSet.contains(entry->getTableID())) {
            entries.push_back(entry);
        }
    }
}

TableCatalogEntry* NodeOrRelExpression::getSingleEntry() const {
    // LCOV_EXCL_START
    if (entries.empty()) {
        throw RuntimeException(
            "Trying to access table id in an empty node. This should never happen");
    }
    // LCOV_EXCL_STOP
    return entries[0];
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
