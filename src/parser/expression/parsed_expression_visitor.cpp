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
        visitCaseExprChildren(expr);
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

void ParsedExpressionVisitor::visitCaseExprChildren(const ParsedExpression& expr) {
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

void ParsedSequenceFunctionCollector::visitFunctionExpr(const ParsedExpression* expr) {
    if (expr->getExpressionType() != ExpressionType::FUNCTION) {
        return;
    }
    auto funName = expr->constCast<ParsedFunctionExpression>().getFunctionName();
    if (StringUtils::getUpper(funName) == function::NextValFunction::name) {
        hasSeqUpdate_ = true;
    }
}

} // namespace parser
} // namespace kuzu
