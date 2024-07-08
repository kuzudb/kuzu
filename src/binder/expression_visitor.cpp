#include "binder/expression_visitor.h"

#include "binder/expression/case_expression.h"
#include "binder/expression/function_expression.h"
#include "binder/expression/node_expression.h"
#include "binder/expression/property_expression.h"
#include "binder/expression/rel_expression.h"
#include "binder/expression/subquery_expression.h"
#include "common/cast.h"
#include "common/exception/not_implemented.h"
#include "function/list/vector_list_functions.h"
#include "function/sequence/sequence_functions.h"
#include "function/uuid/vector_uuid_functions.h"

using namespace kuzu::common;

namespace kuzu {
namespace binder {

void ExpressionVisitor::visitSwitch(std::shared_ptr<Expression> expr) {
    switch (expr->expressionType) {
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
    case ExpressionType::FUNCTION: {
        visitFunctionExpr(expr);
    } break;
    case ExpressionType::AGGREGATE_FUNCTION: {
        visitAggFunctionExpr(expr);
    } break;
    case ExpressionType::PROPERTY: {
        visitPropertyExpr(expr);
    } break;
    case ExpressionType::LITERAL: {
        visitLiteralExpr(expr);
    } break;
    case ExpressionType::VARIABLE: {
        visitVariableExpr(expr);
    } break;
    case ExpressionType::PATH: {
        visitPathExpr(expr);
    } break;
    case ExpressionType::PATTERN: {
        visitNodeRelExpr(expr);
    } break;
    case ExpressionType::PARAMETER: {
        visitParamExpr(expr);
    } break;
    case ExpressionType::SUBQUERY: {
        visitSubqueryExpr(expr);
    } break;
    case ExpressionType::CASE_ELSE: {
        visitCaseExpr(expr);
    } break;
    case ExpressionType::GRAPH: {
        visitGraphExpr(expr);
    } break;
    case ExpressionType::LAMBDA: {
        visitLambdaExpr(expr);
    } break;
        // LCOV_EXCL_START
    default:
        throw NotImplementedException("ExpressionVisitor::visitSwitch");
        // LCOV_EXCL_STOP
    }
}

void ExpressionVisitor::visit(std::shared_ptr<Expression> expr) {
    visitChildren(*expr);
    visitSwitch(expr);
}

void ExpressionVisitor::visitChildren(const Expression& expr) {
    switch (expr.expressionType) {
    case ExpressionType::CASE_ELSE: {
        visitCaseExprChildren(expr);
    } break;
    default: {
        for (auto& child : expr.getChildren()) {
            visit(child);
        }
    }
    }
}

void ExpressionVisitor::visitCaseExprChildren(const Expression& expr) {
    auto& caseExpr = expr.constCast<CaseExpression>();
    for (auto i = 0u; i < caseExpr.getNumCaseAlternatives(); ++i) {
        auto caseAlternative = caseExpr.getCaseAlternative(i);
        visit(caseAlternative->whenExpression);
        visit(caseAlternative->thenExpression);
    }
    visit(caseExpr.getElseExpression());
}

expression_vector ExpressionChildrenCollector::collectChildren(const Expression& expression) {
    switch (expression.expressionType) {
    case ExpressionType::CASE_ELSE: {
        return collectCaseChildren(expression);
    }
    case ExpressionType::SUBQUERY: {
        return collectSubqueryChildren(expression);
    }
    case ExpressionType::PATTERN: {
        switch (expression.dataType.getLogicalTypeID()) {
        case LogicalTypeID::NODE: {
            return collectNodeChildren(expression);
        }
        case LogicalTypeID::REL: {
            return collectRelChildren(expression);
        }
        default: {
            return expression_vector{};
        }
        }
    }
    default: {
        return expression.children;
    }
    }
}

expression_vector ExpressionChildrenCollector::collectCaseChildren(const Expression& expression) {
    expression_vector result;
    auto& caseExpression = (CaseExpression&)expression;
    for (auto i = 0u; i < caseExpression.getNumCaseAlternatives(); ++i) {
        auto caseAlternative = caseExpression.getCaseAlternative(i);
        result.push_back(caseAlternative->whenExpression);
        result.push_back(caseAlternative->thenExpression);
    }
    result.push_back(caseExpression.getElseExpression());
    return result;
}

expression_vector ExpressionChildrenCollector::collectSubqueryChildren(
    const Expression& expression) {
    expression_vector result;
    auto& subqueryExpression = (SubqueryExpression&)expression;
    for (auto& node : subqueryExpression.getQueryGraphCollection()->getQueryNodes()) {
        result.push_back(node->getInternalID());
    }
    if (subqueryExpression.hasWhereExpression()) {
        result.push_back(subqueryExpression.getWhereExpression());
    }
    return result;
}

expression_vector ExpressionChildrenCollector::collectNodeChildren(const Expression& expression) {
    expression_vector result;
    auto& node = expression.constCast<NodeExpression>();
    for (auto& property : node.getPropertyExprs()) {
        result.push_back(property);
    }
    result.push_back(node.getInternalID());
    return result;
}

expression_vector ExpressionChildrenCollector::collectRelChildren(const Expression& expression) {
    expression_vector result;
    auto& rel = expression.constCast<RelExpression>();
    result.push_back(rel.getSrcNode()->getInternalID());
    result.push_back(rel.getDstNode()->getInternalID());
    for (auto& property : rel.getPropertyExprs()) {
        result.push_back(property);
    }
    if (rel.hasDirectionExpr()) {
        result.push_back(rel.getDirectionExpr());
    }
    return result;
}

// isConstant requires all children to be constant.
bool ExpressionVisitor::isConstant(const Expression& expression) {
    if (expression.expressionType == ExpressionType::AGGREGATE_FUNCTION) {
        return false; // We don't have a framework to fold aggregated constant.
    }
    // TODO(Xiyang): this is a bypass to allow nextval to not be folded during binding
    if (expression.expressionType == ExpressionType::FUNCTION) {
        auto& funcExpr = expression.constCast<FunctionExpression>();
        if (funcExpr.getFunctionName() == function::NextValFunction::name) {
            return false;
        }
    }
    if (expression.getNumChildren() == 0 &&
        expression.expressionType != ExpressionType::CASE_ELSE) {
        // If a function does not have children, we should be able to evaluate them as a constant.
        // But I wanna apply this change separately.
        if (expression.expressionType == ExpressionType::FUNCTION) {
            auto& funcExpr = expression.constCast<FunctionExpression>();
            if (funcExpr.getFunctionName() == function::ListCreationFunction::name) {
                return true;
            }
            return false;
        }
        return expression.expressionType == ExpressionType::LITERAL;
    }
    for (auto& child : ExpressionChildrenCollector::collectChildren(expression)) {
        if (!isConstant(*child)) {
            return false;
        }
    }
    return true;
}

bool ExpressionVisitor::isRandom(const Expression& expression) {
    if (expression.expressionType != ExpressionType::FUNCTION) {
        return false;
    }
    auto& funcExpr = ku_dynamic_cast<const Expression&, const FunctionExpression&>(expression);
    if (funcExpr.getFunctionName() == function::GenRandomUUIDFunction::name) {
        return true;
    }
    for (auto& child : ExpressionChildrenCollector::collectChildren(expression)) {
        if (isRandom(*child)) {
            return true;
        }
    }
    return false;
}

void DependentVarNameCollector::visitSubqueryExpr(std::shared_ptr<Expression> expr) {
    auto& subqueryExpr = expr->constCast<SubqueryExpression>();
    for (auto& node : subqueryExpr.getQueryGraphCollection()->getQueryNodes()) {
        varNames.insert(node->getUniqueName());
    }
    if (subqueryExpr.hasWhereExpression()) {
        visit(subqueryExpr.getWhereExpression());
    }
}

void DependentVarNameCollector::visitPropertyExpr(std::shared_ptr<Expression> expr) {
    varNames.insert(expr->constCast<PropertyExpression>().getVariableName());
}

void DependentVarNameCollector::visitNodeRelExpr(std::shared_ptr<Expression> expr) {
    varNames.insert(expr->getUniqueName());
    if (expr->getDataType().getLogicalTypeID() == LogicalTypeID::REL) {
        auto& rel = expr->constCast<RelExpression>();
        varNames.insert(rel.getSrcNodeName());
        varNames.insert(rel.getDstNodeName());
    }
}

void DependentVarNameCollector::visitVariableExpr(std::shared_ptr<Expression> expr) {
    varNames.insert(expr->getUniqueName());
}

void PropertyExprCollector::visitSubqueryExpr(std::shared_ptr<Expression> expr) {
    auto& subqueryExpr = expr->constCast<SubqueryExpression>();
    for (auto& rel : subqueryExpr.getQueryGraphCollection()->getQueryRels()) {
        if (rel->isEmpty() || rel->getRelType() != QueryRelType::NON_RECURSIVE) {
            // If a query rel is empty then it does not have an internal id property.
            continue;
        }
        expressions.push_back(rel->getInternalIDProperty());
    }
    if (subqueryExpr.hasWhereExpression()) {
        visit(subqueryExpr.getWhereExpression());
    }
}

void PropertyExprCollector::visitPropertyExpr(std::shared_ptr<Expression> expr) {
    expressions.push_back(expr);
}

void PropertyExprCollector::visitNodeRelExpr(std::shared_ptr<Expression> expr) {
    for (auto& property : expr->constCast<NodeOrRelExpression>().getPropertyExprs()) {
        expressions.push_back(property);
    }
}

} // namespace binder
} // namespace kuzu
