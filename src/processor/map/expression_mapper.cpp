#include "processor/expression_mapper.h"

#include "binder/expression/case_expression.h"
#include "binder/expression/expression_util.h"
#include "binder/expression/literal_expression.h"
#include "binder/expression/node_expression.h"
#include "binder/expression/parameter_expression.h"
#include "binder/expression/rel_expression.h"
#include "binder/expression_visitor.h" // IWYU pragma: keep (used in assert)
#include "common/exception/not_implemented.h"
#include "common/string_format.h"
#include "expression_evaluator/case_evaluator.h"
#include "expression_evaluator/function_evaluator.h"
#include "expression_evaluator/literal_evaluator.h"
#include "expression_evaluator/node_rel_evaluator.h"
#include "expression_evaluator/path_evaluator.h"
#include "expression_evaluator/reference_evaluator.h"
#include "planner/operator/schema.h"

using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::evaluator;
using namespace kuzu::planner;

namespace kuzu {
namespace processor {

static bool canEvaluateAsFunction(ExpressionType expressionType) {
    switch (expressionType) {
    case ExpressionType::OR:
    case ExpressionType::XOR:
    case ExpressionType::AND:
    case ExpressionType::NOT:
    case ExpressionType::EQUALS:
    case ExpressionType::NOT_EQUALS:
    case ExpressionType::GREATER_THAN:
    case ExpressionType::GREATER_THAN_EQUALS:
    case ExpressionType::LESS_THAN:
    case ExpressionType::LESS_THAN_EQUALS:
    case ExpressionType::IS_NULL:
    case ExpressionType::IS_NOT_NULL:
    case ExpressionType::FUNCTION:
        return true;
    default:
        return false;
    }
}

std::unique_ptr<ExpressionEvaluator> ExpressionMapper::getEvaluator(
    const std::shared_ptr<Expression>& expression, const Schema* schema) {
    if (schema == nullptr) {
        return getConstantEvaluator(expression);
    }
    auto expressionType = expression->expressionType;
    if (schema->isExpressionInScope(*expression)) {
        return getReferenceEvaluator(expression, schema);
    } else if (isExpressionLiteral(expressionType)) {
        return getLiteralEvaluator(*expression);
    } else if (ExpressionUtil::isNodePattern(*expression)) {
        return getNodeEvaluator(expression, schema);
    } else if (ExpressionUtil::isRelPattern(*expression)) {
        return getRelEvaluator(expression, schema);
    } else if (expressionType == ExpressionType::PATH) {
        return getPathEvaluator(expression, schema);
    } else if (expressionType == ExpressionType::PARAMETER) {
        return getParameterEvaluator(*expression);
    } else if (ExpressionType::CASE_ELSE == expressionType) {
        return getCaseEvaluator(expression, schema);
    } else if (canEvaluateAsFunction(expressionType)) {
        return getFunctionEvaluator(expression, schema);
    } else {
        // LCOV_EXCL_START
        throw NotImplementedException(stringFormat("Cannot evaluate expression with type {}.",
            expressionTypeToString(expressionType)));
        // LCOV_EXCL_STOP
    }
}

std::unique_ptr<ExpressionEvaluator> ExpressionMapper::getConstantEvaluator(
    const std::shared_ptr<Expression>& expression) {
    KU_ASSERT(ExpressionVisitor::isConstant(*expression));
    auto expressionType = expression->expressionType;
    if (isExpressionLiteral(expressionType)) {
        return getLiteralEvaluator(*expression);
    } else if (ExpressionType::CASE_ELSE == expressionType) {
        return getCaseEvaluator(expression, nullptr);
    } else if (canEvaluateAsFunction(expressionType)) {
        return getFunctionEvaluator(expression, nullptr);
    } else {
        // LCOV_EXCL_START
        throw NotImplementedException(stringFormat("Cannot evaluate expression with type {}.",
            expressionTypeToString(expressionType)));
        // LCOV_EXCL_STOP
    }
}

std::unique_ptr<ExpressionEvaluator> ExpressionMapper::getLiteralEvaluator(
    const Expression& expression) {
    auto& literalExpression = expression.constCast<LiteralExpression>();
    return std::make_unique<LiteralExpressionEvaluator>(literalExpression.getValue());
}

std::unique_ptr<ExpressionEvaluator> ExpressionMapper::getParameterEvaluator(
    const Expression& expression) {
    auto& parameterExpression = expression.constCast<ParameterExpression>();
    return std::make_unique<LiteralExpressionEvaluator>(parameterExpression.getValue());
}

std::unique_ptr<ExpressionEvaluator> ExpressionMapper::getReferenceEvaluator(
    const std::shared_ptr<Expression>& expression, const Schema* schema) {
    KU_ASSERT(schema != nullptr);
    auto vectorPos = DataPos(schema->getExpressionPos(*expression));
    auto expressionGroup = schema->getGroup(expression->getUniqueName());
    return std::make_unique<ReferenceExpressionEvaluator>(vectorPos, expressionGroup->isFlat());
}

std::unique_ptr<ExpressionEvaluator> ExpressionMapper::getCaseEvaluator(
    std::shared_ptr<Expression> expression, const Schema* schema) {
    auto caseExpression = reinterpret_cast<CaseExpression*>(expression.get());
    std::vector<std::unique_ptr<CaseAlternativeEvaluator>> alternativeEvaluators;
    for (auto i = 0u; i < caseExpression->getNumCaseAlternatives(); ++i) {
        auto alternative = caseExpression->getCaseAlternative(i);
        auto whenEvaluator = getEvaluator(alternative->whenExpression, schema);
        auto thenEvaluator = getEvaluator(alternative->thenExpression, schema);
        alternativeEvaluators.push_back(std::make_unique<CaseAlternativeEvaluator>(
            std::move(whenEvaluator), std::move(thenEvaluator)));
    }
    auto elseEvaluator = getEvaluator(caseExpression->getElseExpression(), schema);
    return std::make_unique<CaseExpressionEvaluator>(std::move(expression),
        std::move(alternativeEvaluators), std::move(elseEvaluator));
}

std::unique_ptr<ExpressionEvaluator> ExpressionMapper::getFunctionEvaluator(
    std::shared_ptr<Expression> expression, const Schema* schema) {
    auto childrenEvaluators = getEvaluators(expression->getChildren(), schema);
    return std::make_unique<FunctionExpressionEvaluator>(std::move(expression),
        std::move(childrenEvaluators));
}

std::unique_ptr<ExpressionEvaluator> ExpressionMapper::getNodeEvaluator(
    std::shared_ptr<Expression> expression, const Schema* schema) {
    auto node = (NodeExpression*)expression.get();
    expression_vector children;
    children.push_back(node->getInternalID());
    children.push_back(node->getLabelExpression());
    for (auto& property : node->getPropertyExprs()) {
        children.push_back(property);
    }
    auto childrenEvaluators = getEvaluators(children, schema);
    return std::make_unique<NodeRelExpressionEvaluator>(std::move(expression),
        std::move(childrenEvaluators));
}

std::unique_ptr<ExpressionEvaluator> ExpressionMapper::getRelEvaluator(
    std::shared_ptr<Expression> expression, const Schema* schema) {
    auto rel = (RelExpression*)expression.get();
    expression_vector children;
    children.push_back(rel->getSrcNode()->getInternalID());
    children.push_back(rel->getDstNode()->getInternalID());
    children.push_back(rel->getLabelExpression());
    for (auto& property : rel->getPropertyExprs()) {
        children.push_back(property);
    }
    auto childrenEvaluators = getEvaluators(children, schema);
    return std::make_unique<NodeRelExpressionEvaluator>(std::move(expression),
        std::move(childrenEvaluators));
}

std::unique_ptr<ExpressionEvaluator> ExpressionMapper::getPathEvaluator(
    std::shared_ptr<Expression> expression, const Schema* schema) {
    auto childrenEvaluators = getEvaluators(expression->getChildren(), schema);
    return std::make_unique<PathExpressionEvaluator>(std::move(expression),
        std::move(childrenEvaluators));
}

std::vector<std::unique_ptr<ExpressionEvaluator>> ExpressionMapper::getEvaluators(
    const binder::expression_vector& expressions, const Schema* schema) {
    std::vector<std::unique_ptr<ExpressionEvaluator>> evaluators;
    evaluators.reserve(expressions.size());
    for (auto& expression : expressions) {
        evaluators.push_back(getEvaluator(expression, schema));
    }
    return evaluators;
}

} // namespace processor
} // namespace kuzu
