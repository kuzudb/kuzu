#include "binder/rewriter/with_clause_projection_rewriter.h"

#include "binder/expression/property_expression.h"
#include "binder/visitor/property_collector.h"

using namespace kuzu::common;

namespace kuzu {
namespace binder {

static expression_vector getPropertiesOfSameVariable(
    const expression_vector& expressions, const std::string& variableName) {
    expression_vector result;
    for (auto& expression : expressions) {
        auto propertyExpression = (PropertyExpression*)expression.get();
        if (propertyExpression->getVariableName() != variableName) {
            continue;
        }
        result.push_back(expression);
    }
    return result;
}

static expression_vector rewriteExpressions(
    const expression_vector& expressions, const expression_vector& properties) {
    expression_set distinctResult;
    for (auto& expression : expressions) {
        if (expression->expressionType != common::ExpressionType::PROPERTY) {
            distinctResult.insert(expression);
            continue;
        }
        auto propertyExpression = (PropertyExpression*)expression.get();
        if (!propertyExpression->isInternalID()) {
            distinctResult.insert(expression);
            continue;
        }
        // Expression is internal ID. Perform rewrite as all properties with the same variable.
        auto variableName = propertyExpression->getVariableName();
        for (auto& property : getPropertiesOfSameVariable(properties, variableName)) {
            distinctResult.insert(property);
        }
    }
    return expression_vector{distinctResult.begin(), distinctResult.end()};
}

void WithClauseProjectionRewriter::visitSingleQuery(const NormalizedSingleQuery& singleQuery) {
    auto propertyCollector = PropertyCollector();
    propertyCollector.visitSingleQuery(singleQuery);
    auto properties = propertyCollector.getProperties();
    for (auto i = 0; i < singleQuery.getNumQueryParts() - 1; ++i) {
        auto queryPart = singleQuery.getQueryPart(i);
        auto projectionBody = queryPart->getProjectionBody();
        auto newProjectionExpressions =
            rewriteExpressions(projectionBody->getProjectionExpressions(), properties);
        projectionBody->setProjectionExpressions(std::move(newProjectionExpressions));
        auto newGroupByExpressions =
            rewriteExpressions(projectionBody->getGroupByExpressions(), properties);
        projectionBody->setGroupByExpressions(std::move(newGroupByExpressions));
    }
}

} // namespace binder
} // namespace kuzu
