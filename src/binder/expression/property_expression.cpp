#include "binder/expression/property_expression.h"

#include "binder/expression/node_rel_expression.h"

using namespace kuzu::common;

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
    for (auto& tableID : patternExpr.getTableIDs()) {
        infos.insert({tableID, SingleLabelPropertyInfo(false, INVALID_PROPERTY_ID)});
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

bool PropertyExpression::hasPropertyID(common::table_id_t tableID) const {
    // For RDF, we mock the existence of iri on resource triples table. So we cannot assert infos
    // always contain tableID.
    if (!infos.contains(tableID)) {
        return false;
    }
    return infos.at(tableID).id != INVALID_PROPERTY_ID;
}

common::property_id_t PropertyExpression::getPropertyID(common::table_id_t tableID) const {
    KU_ASSERT(infos.contains(tableID));
    return infos.at(tableID).id;
}

} // namespace binder
} // namespace kuzu
