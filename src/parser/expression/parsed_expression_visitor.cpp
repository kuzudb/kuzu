#include "parser/expression/parsed_expression_visitor.h"

#include "common/exception/not_implemented.h"
#include "function/sequence/sequence_functions.h"
#include "parser/expression/parsed_case_expression.h"
#include "parser/expression/parsed_function_expression.h"
#include "parser/expression/parsed_lambda_expression.h"

using namespace kuzu::common;

namespace kuzu {
namespace parser {

void ParsedExpressionVisitor::visit(const ParsedExpression* expr) {
    visitChildren(*expr);
    visitSwitch(expr);
}

void ParsedExpressionVisitor::visitUnsafe(ParsedExpression* expr) {
    visitChildrenUnsafe(*expr);
    visitSwitchUnsafe(expr);
}

void ParsedExpressionVisitor::visitSwitch(const ParsedExpression* expr) {
    switch (expr->getExpressionType()) {
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
    case ExpressionType::STAR: {
        visitStar(expr);
    } break;
        // LCOV_EXCL_START
    default:
        throw NotImplementedException("ExpressionVisitor::visitSwitch");
        // LCOV_EXCL_STOP
    }
}

void ParsedExpressionVisitor::visitChildren(const ParsedExpression& expr) {
    switch (expr.getExpressionType()) {
    case ExpressionType::CASE_ELSE: {
        visitCaseChildren(expr);
    } break;
    case ExpressionType::LAMBDA: {
        auto& lambda = expr.constCast<ParsedLambdaExpression>();
        visit(lambda.getFunctionExpr());
    } break;
    default: {
        for (auto i = 0u; i < expr.getNumChildren(); ++i) {
            visit(expr.getChild(i));
        }
    }
    }
}

void ParsedExpressionVisitor::visitChildrenUnsafe(ParsedExpression& expr) {
    switch (expr.getExpressionType()) {
    case ExpressionType::CASE_ELSE: {
        visitCaseChildrenUnsafe(expr);
    } break;
    default: {
        for (auto i = 0u; i < expr.getNumChildren(); ++i) {
            visitUnsafe(expr.getChild(i));
        }
    }
    }
}

void ParsedExpressionVisitor::visitCaseChildren(const ParsedExpression& expr) {
    auto& caseExpr = expr.constCast<ParsedCaseExpression>();
    if (caseExpr.hasCaseExpression()) {
        visit(caseExpr.getCaseExpression());
    }
    for (auto i = 0u; i < caseExpr.getNumCaseAlternative(); ++i) {
        auto alternative = caseExpr.getCaseAlternative(i);
        visit(alternative->whenExpression.get());
        visit(alternative->thenExpression.get());
    }
    if (caseExpr.hasElseExpression()) {
        visit(caseExpr.getElseExpression());
    }
}

void ParsedExpressionVisitor::visitCaseChildrenUnsafe(ParsedExpression& expr) {
    auto& caseExpr = expr.cast<ParsedCaseExpression>();
    if (caseExpr.hasCaseExpression()) {
        visitUnsafe(caseExpr.getCaseExpression());
    }
    for (auto i = 0u; i < caseExpr.getNumCaseAlternative(); ++i) {
        auto alternative = caseExpr.getCaseAlternative(i);
        visitUnsafe(alternative->whenExpression.get());
        visitUnsafe(alternative->thenExpression.get());
    }
    if (caseExpr.hasElseExpression()) {
        visitUnsafe(caseExpr.getElseExpression());
    }
}

void ParsedSequenceFunctionCollector::visitFunctionExpr(const ParsedExpression* expr) {
    if (expr->getExpressionType() != ExpressionType::FUNCTION) {
        return;
    }
    auto funName = expr->constCast<ParsedFunctionExpression>().getFunctionName();
    if (StringUtils::getUpper(funName) == function::NextValFunction::name) {
        hasSeqUpdate_ = true;
    }
}

std::unique_ptr<ParsedExpression> MacroParameterReplacer::replace(
    std::unique_ptr<ParsedExpression> input) {
    if (nameToExpr.contains(input->getRawName())) {
        return nameToExpr.at(input->getRawName())->copy();
    }
    visitUnsafe(input.get());
    return input;
}

void MacroParameterReplacer::visitSwitchUnsafe(ParsedExpression* expr) {
    switch (expr->getExpressionType()) {
    case ExpressionType::CASE_ELSE: {
        auto& caseExpr = expr->cast<ParsedCaseExpression>();
        if (caseExpr.hasCaseExpression()) {
            auto replace = getReplace(caseExpr.getCaseExpression()->getRawName());
            if (replace) {
                caseExpr.setCaseExpression(std::move(replace));
            }
        }
        for (auto i = 0u; i < caseExpr.getNumCaseAlternative(); i++) {
            auto caseAlternative = caseExpr.getCaseAlternativeUnsafe(i);
            auto whenReplace = getReplace(caseAlternative->whenExpression->getRawName());
            auto thenReplace = getReplace(caseAlternative->thenExpression->getRawName());
            if (whenReplace) {
                caseAlternative->whenExpression = std::move(whenReplace);
            }
            if (thenReplace) {
                caseAlternative->thenExpression = std::move(thenReplace);
            }
        }
        if (caseExpr.hasElseExpression()) {
            auto replace = getReplace(caseExpr.getElseExpression()->getRawName());
            if (replace) {
                caseExpr.setElseExpression(std::move(replace));
            }
        }
    } break;
    default: {
        for (auto i = 0u; i < expr->getNumChildren(); ++i) {
            auto child = expr->getChild(i);
            auto replace = getReplace(child->getRawName());
            if (replace) {
                expr->setChild(i, std::move(replace));
            }
        }
    }
    }
}

std::unique_ptr<ParsedExpression> MacroParameterReplacer::getReplace(const std::string& name) {
    if (nameToExpr.contains(name)) {
        return nameToExpr.at(name)->copy();
    }
    return nullptr;
}

} // namespace parser
} // namespace kuzu
