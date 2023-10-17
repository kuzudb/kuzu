#include "binder/expression/expression_util.h"
#include "binder/expression/rel_expression.h"
#include "binder/expression_binder.h"
#include "common/exception/binder.h"
#include "common/string_format.h"
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
        throw BinderException(stringFormat(
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
    if (ExpressionUtil::isNodeVariable(*child) || ExpressionUtil::isRelVariable(*child)) {
        return bindNodeOrRelPropertyExpression(*child, propertyName);
    }
    return bindStructPropertyExpression(child, propertyName);
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
