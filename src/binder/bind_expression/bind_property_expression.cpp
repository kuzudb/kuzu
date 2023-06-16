#include "binder/expression/literal_expression.h"
#include "binder/expression/rel_expression.h"
#include "binder/expression_binder.h"
#include "parser/expression/parsed_property_expression.h"

using namespace kuzu::common;
using namespace kuzu::parser;
using namespace kuzu::catalog;

namespace kuzu {
namespace binder {

std::shared_ptr<Expression> ExpressionBinder::bindPropertyExpression(
    const ParsedExpression& parsedExpression) {
    auto& propertyExpression = (ParsedPropertyExpression&)parsedExpression;
    auto propertyName = propertyExpression.getPropertyName();
    if (TableSchema::isReservedPropertyName(propertyName)) {
        // Note we don't expose direct access to internal properties in case user tries to modify
        // them. However, we can expose indirect read-only access through function e.g. ID().
        throw BinderException(
            propertyName + " is reserved for system usage. External access is not allowed.");
    }
    auto child = bindExpression(*parsedExpression.getChild(0));
    validateExpectedDataType(*child,
        std::vector<LogicalTypeID>{LogicalTypeID::NODE, LogicalTypeID::REL, LogicalTypeID::STRUCT});
    auto childTypeID = child->dataType.getLogicalTypeID();
    if (LogicalTypeID::NODE == childTypeID) {
        return bindNodePropertyExpression(*child, propertyName);
    } else if (LogicalTypeID::REL == childTypeID) {
        return bindRelPropertyExpression(*child, propertyName);
    } else {
        assert(LogicalTypeID::STRUCT == childTypeID);
        auto stringValue =
            std::make_unique<Value>(LogicalType{LogicalTypeID::STRING}, propertyName);
        return bindScalarFunctionExpression(
            expression_vector{child, createLiteralExpression(std::move(stringValue))},
            STRUCT_EXTRACT_FUNC_NAME);
    }
}

std::shared_ptr<Expression> ExpressionBinder::bindNodePropertyExpression(
    const Expression& expression, const std::string& propertyName) {
    auto& nodeOrRel = (NodeOrRelExpression&)expression;
    if (!nodeOrRel.hasPropertyExpression(propertyName)) {
        throw BinderException(
            "Cannot find property " + propertyName + " for " + expression.toString() + ".");
    }
    return nodeOrRel.getPropertyExpression(propertyName);
}

static void validatePropertiesWithSameDataType(const std::vector<Property>& properties,
    const LogicalType& dataType, const std::string& propertyName, const std::string& variableName) {
    for (auto& property : properties) {
        if (property.dataType != dataType) {
            throw BinderException(
                "Cannot resolve data type of " + propertyName + " for " + variableName + ".");
        }
    }
}

static std::unordered_map<table_id_t, property_id_t> populatePropertyIDPerTable(
    const std::vector<Property>& properties) {
    std::unordered_map<table_id_t, property_id_t> propertyIDPerTable;
    for (auto& property : properties) {
        propertyIDPerTable.insert({property.tableID, property.propertyID});
    }
    return propertyIDPerTable;
}

std::shared_ptr<Expression> ExpressionBinder::bindRelPropertyExpression(
    const Expression& expression, const std::string& propertyName) {
    auto& rel = (RelExpression&)expression;
    if (!rel.hasPropertyExpression(propertyName)) {
        throw BinderException(
            "Cannot find property " + propertyName + " for " + expression.toString() + ".");
    }
    return rel.getPropertyExpression(propertyName);
}

std::unique_ptr<Expression> ExpressionBinder::createPropertyExpression(
    const Expression& nodeOrRel, const std::vector<Property>& properties, bool isPrimaryKey) {
    assert(!properties.empty());
    auto anchorProperty = properties[0];
    validatePropertiesWithSameDataType(
        properties, anchorProperty.dataType, anchorProperty.name, nodeOrRel.toString());
    return make_unique<PropertyExpression>(anchorProperty.dataType, anchorProperty.name, nodeOrRel,
        populatePropertyIDPerTable(properties), isPrimaryKey);
}

} // namespace binder
} // namespace kuzu
