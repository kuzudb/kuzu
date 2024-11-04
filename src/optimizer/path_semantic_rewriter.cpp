#include "optimizer/path_semantic_rewriter.h"

#include "binder/expression/path_expression.h"
#include "binder/expression/scalar_function_expression.h"
#include "binder/expression_visitor.h"
#include "catalog/catalog.h"
#include "common/exception/internal.h"
#include "function/built_in_function_utils.h"
#include "function/path/vector_path_functions.h"
#include "function/scalar_function.h"
#include "main/client_context.h"
#include "planner/operator/logical_filter.h"
using namespace kuzu::common;
using namespace kuzu::planner;

namespace kuzu {
namespace optimizer {

void PathSemanticRewriter::rewrite(planner::LogicalPlan* plan) {
    auto root = plan->getLastOperator();
    visitOperator(root, nullptr, 0);
}

void PathSemanticRewriter::visitOperator(const std::shared_ptr<planner::LogicalOperator>& op,
    const std::shared_ptr<planner::LogicalOperator>& parent, int index) {

    auto result = op;
    switch (op->getOperatorType()) {
    case planner::LogicalOperatorType::HASH_JOIN:
        result = visitHashJoinReplace(op);
        break;
    case planner::LogicalOperatorType::CROSS_PRODUCT:
        result = visitCrossProductReplace(op);
        break;
    default:
        break;
    }

    if (hasReplace && parent != nullptr) {
        parent->setChild(index, result);

    } else {
        for (auto i = 0u; i < op->getNumChildren(); ++i) {
            visitOperator(op->getChild(i), op, i);
        }
    }
}

std::string semanticSwitch(const common::PathSemantic& semantic) {
    switch (semantic) {
    case common::PathSemantic::TRAIL:
        return function::IsTrailFunction::name;
    case common::PathSemantic::ACYCLIC:
        return function::IsACyclicFunction::name;
    default:
        return std::string();
    }
}

std::shared_ptr<LogicalOperator> PathSemanticRewriter::visitHashJoinReplace(
    std::shared_ptr<LogicalOperator> op) {
    return appendPathSemanticFilter(op);
}

std::shared_ptr<LogicalOperator> PathSemanticRewriter::appendPathSemanticFilter(
    const std::shared_ptr<LogicalOperator> op) {
    // get path expression from binder
    auto pathExpressions = context->getBinder()->findPathExpressionInScope();
    auto catalog = context->getCatalog();
    auto transaction = context->getTx();
    auto semanticFunctionName =
        semanticSwitch(context->getClientConfig()->recursivePatternSemantic);
    if (semanticFunctionName.empty()) {
        return op;
    }

    auto resultOp = op;
    // append is_trail or is_acyclic function filter
    for (auto& expr : pathExpressions) {

        std::vector<LogicalType> childrenTypes;
        childrenTypes.push_back(expr->getDataType().copy());

        auto bindExpr = binder::expression_vector{expr};
        auto functions = catalog->getFunctions(transaction);
        auto function = function::BuiltInFunctionsUtils::matchFunction(transaction,
            semanticFunctionName, childrenTypes, functions)
                            ->ptrCast<function::ScalarFunction>()
                            ->copy();

        std::unique_ptr<function::FunctionBindData> bindData;
        {
            if (function.bindFunc) {
                bindData = function.bindFunc({bindExpr, &function, context});
            } else {
                bindData = std::make_unique<function::FunctionBindData>(
                    LogicalType(function.returnTypeID));
            }
        }

        auto uniqueExpressionName =
            binder::ScalarFunctionExpression::getUniqueName(function.name, bindExpr);
        auto filterExpression =
            std::make_shared<binder::ScalarFunctionExpression>(ExpressionType::FUNCTION,
                std::move(function), std::move(bindData), bindExpr, uniqueExpressionName);
        auto printInfo = std::make_unique<OPPrintInfo>();

        auto filter = std::make_shared<LogicalFilter>(
            std::static_pointer_cast<binder::Expression>(filterExpression), resultOp,
            std::move(printInfo));
        hasReplace = true;
        filter->computeFlatSchema();
        resultOp = filter;
    }
    return resultOp;
}
std::shared_ptr<planner::LogicalOperator> PathSemanticRewriter::visitCrossProductReplace(
    std::shared_ptr<planner::LogicalOperator> op) {
    return appendPathSemanticFilter(op);
}

} // namespace optimizer
} // namespace kuzu
