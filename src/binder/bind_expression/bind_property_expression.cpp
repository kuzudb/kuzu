#include "binder/expression/rel_expression.h"
#include "binder/expression_binder.h"
#include "common/string_utils.h"
#include "parser/expression/parsed_property_expression.h"

using namespace kuzu::common;
using namespace kuzu::parser;
using namespace kuzu::catalog;

namespace kuzu {
namespace binder {

expression_vector ExpressionBinder::bindPropertyStarExpression(
    const parser::ParsedExpression& parsedExpression) {
    auto child = bindExpression(*parsedExpression.getChild(0));
    validateExpectedDataType(*child,
        std::vector<LogicalTypeID>{LogicalTypeID::NODE, LogicalTypeID::REL, LogicalTypeID::STRUCT});
    if (ExpressionUtil::isNodeVariable(*child)) {
        return bindNodePropertyStarExpression(*child);
    } else if (ExpressionUtil::isRelVariable(*child)) {
        return bindRelPropertyStarExpression(*child);
    } else {
        return bindStructPropertyStarExpression(child);
    }
}

expression_vector ExpressionBinder::bindNodePropertyStarExpression(const Expression& child) {
    expression_vector result;
    auto& node = (NodeExpression&)child;
    for (auto& property : node.getPropertyExpressions()) {
        result.push_back(property->copy());
    }
    return result;
}

expression_vector ExpressionBinder::bindRelPropertyStarExpression(const Expression& child) {
    expression_vector result;
    auto& node = (RelExpression&)child;
    for (auto& property : node.getPropertyExpressions()) {
        auto propertyExpression = (PropertyExpression*)property.get();
        if (TableSchema::isReservedPropertyName(propertyExpression->getPropertyName())) {
            continue;
        }
        result.push_back(property->copy());
    }
    return result;
}

expression_vector ExpressionBinder::bindStructPropertyStarExpression(
    std::shared_ptr<Expression> child) {
    assert(child->getDataType().getLogicalTypeID() == LogicalTypeID::STRUCT);
    expression_vector result;
    auto childType = child->getDataType();
    for (auto field : StructType::getFields(&childType)) {
        result.push_back(bindStructPropertyExpression(child, field->getName()));
    }
    return result;
}

std::shared_ptr<Expression> ExpressionBinder::bindPropertyExpression(
    const ParsedExpression& parsedExpression) {
    auto& propertyExpression = (ParsedPropertyExpression&)parsedExpression;
    if (propertyExpression.isStar()) {
        throw BinderException(StringUtils::string_format(
            "Cannot bind {} as a single property expression.", parsedExpression.toString()));
    }
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
    if (ExpressionUtil::isNodeVariable(*child)) {
        return bindNodePropertyExpression(*child, propertyName);
    } else if (ExpressionUtil::isRelVariable(*child)) {
        return bindRelPropertyExpression(*child, propertyName);
    } else {
        assert(child->expressionType == FUNCTION);
        return bindStructPropertyExpression(child, propertyName);
    }
}

std::shared_ptr<Expression> ExpressionBinder::bindNodePropertyExpression(
    const Expression& child, const std::string& propertyName) {
    auto& node = (NodeExpression&)child;
    if (!node.hasPropertyExpression(propertyName)) {
        throw BinderException(
            "Cannot find property " + propertyName + " for " + child.toString() + ".");
    }
    return node.getPropertyExpression(propertyName);
}

std::shared_ptr<Expression> ExpressionBinder::bindRelPropertyExpression(
    const Expression& child, const std::string& propertyName) {
    auto& rel = (RelExpression&)child;
    if (!rel.hasPropertyExpression(propertyName)) {
        throw BinderException(
            "Cannot find property " + propertyName + " for " + child.toString() + ".");
    }
    return rel.getPropertyExpression(propertyName);
}

std::shared_ptr<Expression> ExpressionBinder::bindStructPropertyExpression(
    std::shared_ptr<Expression> child, const std::string& propertyName) {
    auto children =
        expression_vector{std::move(child), createStringLiteralExpression(propertyName)};
    return bindScalarFunctionExpression(children, STRUCT_EXTRACT_FUNC_NAME);
}

static void validatePropertiesWithSameDataType(const std::vector<Property*>& properties,
    const LogicalType& dataType, const std::string& propertyName, const std::string& variableName) {
    auto propertyLookup = variableName + "." + propertyName;
    for (auto& property : properties) {
        if (*property->getDataType() != dataType) {
            throw BinderException(
                StringUtils::string_format("Expect one data type for {} but find {} and {}",
                    propertyLookup, LogicalTypeUtils::dataTypeToString(*property->getDataType()),
                    LogicalTypeUtils::dataTypeToString(dataType)));
        }
    }
}

static std::unordered_map<table_id_t, property_id_t> populatePropertyIDPerTable(
    const std::vector<Property*>& properties) {
    std::unordered_map<table_id_t, property_id_t> propertyIDPerTable;
    for (auto& property : properties) {
        propertyIDPerTable.insert({property->getTableID(), property->getPropertyID()});
    }
    return propertyIDPerTable;
}

std::unique_ptr<Expression> ExpressionBinder::createPropertyExpression(
    const Expression& nodeOrRel, const std::vector<Property*>& properties, bool isPrimaryKey) {
    assert(!properties.empty());
    auto anchorProperty = properties[0];
    validatePropertiesWithSameDataType(properties, *anchorProperty->getDataType(),
        anchorProperty->getName(), nodeOrRel.toString());
    return make_unique<PropertyExpression>(*anchorProperty->getDataType(),
        anchorProperty->getName(), nodeOrRel, populatePropertyIDPerTable(properties), isPrimaryKey);
}

} // namespace binder
} // namespace kuzu
