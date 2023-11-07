#include "binder/expression/expression_util.h"
#include "binder/expression/node_rel_expression.h"
#include "binder/expression/property_expression.h"
#include "binder/expression_binder.h"
#include "catalog/table_schema.h"
#include "common/exception/binder.h"
#include "common/string_format.h"
#include "parser/expression/parsed_property_expression.h"

using namespace kuzu::common;
using namespace kuzu::parser;
using namespace kuzu::catalog;

namespace kuzu {
namespace binder {

static bool isNodeOrRelPattern(const Expression& expression) {
    return ExpressionUtil::isNodePattern(expression) || ExpressionUtil::isRelPattern(expression);
}

static bool isStructPattern(const Expression& expression) {
    auto logicalTypeID = expression.getDataType().getLogicalTypeID();
    return logicalTypeID == LogicalTypeID::NODE || logicalTypeID == LogicalTypeID::REL ||
           logicalTypeID == LogicalTypeID::STRUCT;
}

expression_vector ExpressionBinder::bindPropertyStarExpression(
    const parser::ParsedExpression& parsedExpression) {
    auto child = bindExpression(*parsedExpression.getChild(0));
    if (isNodeOrRelPattern(*child)) {
        return bindNodeOrRelPropertyStarExpression(*child);
    } else if (isStructPattern(*child)) {
        return bindStructPropertyStarExpression(child);
    } else {
        throw BinderException(stringFormat("Cannot bind property for expression {} with type {}.",
            child->toString(), expressionTypeToString(child->expressionType)));
    }
}

expression_vector ExpressionBinder::bindNodeOrRelPropertyStarExpression(const Expression& child) {
    expression_vector result;
    auto& nodeOrRel = (NodeOrRelExpression&)child;
    for (auto& expression : nodeOrRel.getPropertyExpressions()) {
        auto propertyExpression = (PropertyExpression*)expression.get();
        if (TableSchema::isReservedPropertyName(propertyExpression->getPropertyName())) {
            continue;
        }
        result.push_back(expression->copy());
    }
    return result;
}

expression_vector ExpressionBinder::bindStructPropertyStarExpression(
    std::shared_ptr<Expression> child) {
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
        throw BinderException(stringFormat(
            "Cannot bind {} as a single property expression.", parsedExpression.toString()));
    }
    auto propertyName = propertyExpression.getPropertyName();
    auto child = bindExpression(*parsedExpression.getChild(0));
    validateExpectedDataType(*child,
        std::vector<LogicalTypeID>{LogicalTypeID::NODE, LogicalTypeID::REL, LogicalTypeID::STRUCT});
    if (isNodeOrRelPattern(*child)) {
        if (TableSchema::isReservedPropertyName(propertyName)) {
            // Note we don't expose direct access to internal properties in case user tries to
            // modify them. However, we can expose indirect read-only access through function e.g.
            // ID().
            throw BinderException(
                propertyName + " is reserved for system usage. External access is not allowed.");
        }
        return bindNodeOrRelPropertyExpression(*child, propertyName);
    } else if (isStructPattern(*child)) {
        return bindStructPropertyExpression(child, propertyName);
    } else {
        throw BinderException(stringFormat("Cannot bind property for expression {} with type {}.",
            child->toString(), expressionTypeToString(child->expressionType)));
    }
}

std::shared_ptr<Expression> ExpressionBinder::bindNodeOrRelPropertyExpression(
    const Expression& child, const std::string& propertyName) {
    auto& nodeOrRel = reinterpret_cast<const NodeOrRelExpression&>(child);
    if (!nodeOrRel.hasPropertyExpression(propertyName)) {
        throw BinderException(
            "Cannot find property " + propertyName + " for " + child.toString() + ".");
    }
    return nodeOrRel.getPropertyExpression(propertyName);
}

std::shared_ptr<Expression> ExpressionBinder::bindStructPropertyExpression(
    std::shared_ptr<Expression> child, const std::string& propertyName) {
    auto children =
        expression_vector{std::move(child), createStringLiteralExpression(propertyName)};
    return bindScalarFunctionExpression(children, STRUCT_EXTRACT_FUNC_NAME);
}

} // namespace binder
} // namespace kuzu
