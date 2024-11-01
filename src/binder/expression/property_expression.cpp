#include "binder/expression/property_expression.h"

#include "binder/expression/node_rel_expression.h"
#include "catalog/catalog_entry/table_catalog_entry.h"

using namespace kuzu::common;
using namespace kuzu::catalog;

namespace kuzu {
namespace binder {

std::unique_ptr<PropertyExpression> PropertyExpression::construct(LogicalType type,
    const std::string& propertyName, const Expression& child) {
    KU_ASSERT(child.expressionType == ExpressionType::PATTERN);
    auto& patternExpr = child.constCast<NodeOrRelExpression>();
    auto variableName = patternExpr.getVariableName();
    auto uniqueName = patternExpr.getUniqueName();
    // Assign an invalid property id for virtual property.
    common::table_id_map_t<SingleLabelPropertyInfo> infos;
    for (auto& entry : patternExpr.getEntries()) {
        infos.insert({entry->getTableID(),
            SingleLabelPropertyInfo(false /* exists */, false /* isPrimaryKey */)});
    }
    return std::make_unique<PropertyExpression>(std::move(type), propertyName, uniqueName,
        variableName, std::move(infos));
}

bool PropertyExpression::isPrimaryKey() const {
    for (auto& [id, info] : infos) {
        if (!info.isPrimaryKey) {
            return false;
        }
    }
    return true;
}

bool PropertyExpression::isPrimaryKey(common::table_id_t tableID) const {
    if (!infos.contains(tableID)) {
        return false;
    }
    return infos.at(tableID).isPrimaryKey;
}

bool PropertyExpression::hasProperty(common::table_id_t tableID) const {
    KU_ASSERT(infos.contains(tableID));
    return infos.at(tableID).exists;
}

column_id_t PropertyExpression::getColumnID(const TableCatalogEntry& entry) const {
    if (!hasProperty(entry.getTableID())) {
        return INVALID_COLUMN_ID;
    }
    return entry.getColumnID(propertyName);
}

} // namespace binder
} // namespace kuzu
