#include "binder/binder.h"
#include "binder/expression/aggregate_function_expression.h"
#include "binder/expression/expression_util.h"
#include "binder/expression/scalar_function_expression.h"
#include "binder/expression_binder.h"
#include "catalog/catalog.h"
#include "common/exception/binder.h"
#include "function/built_in_function_utils.h"
#include "function/cast/vector_cast_functions.h"
#include "function/rewrite_function.h"
#include "function/scalar_macro_function.h"
#include "function/schema/vector_label_functions.h"
#include "function/schema/vector_node_rel_functions.h"
#include "main/client_context.h"
#include "parser/expression/parsed_function_expression.h"
#include "parser/parsed_expression_visitor.h"

using namespace kuzu::common;
using namespace kuzu::parser;
using namespace kuzu::function;
using namespace kuzu::catalog;

namespace kuzu {
namespace binder {

std::shared_ptr<Expression> ExpressionBinder::bindFunctionExpression(const ParsedExpression& expr) {
    auto funcExpr = expr.constPtrCast<ParsedFunctionExpression>();
    auto functionName = funcExpr->getNormalizedFunctionName();
    auto result = rewriteFunctionExpression(expr, functionName);
    if (result != nullptr) {
        return result;
    }
    auto entry = context->getCatalog()->getFunctionEntry(context->getTransaction(), functionName);
    switch (entry->getType()) {
    case CatalogEntryType::SCALAR_FUNCTION_ENTRY:
        return bindScalarFunctionExpression(expr, functionName);
    case CatalogEntryType::REWRITE_FUNCTION_ENTRY:
        return bindRewriteFunctionExpression(expr);
    case CatalogEntryType::AGGREGATE_FUNCTION_ENTRY:
        return bindAggregateFunctionExpression(expr, functionName, funcExpr->getIsDistinct());
    case CatalogEntryType::SCALAR_MACRO_ENTRY:
        return bindMacroExpression(expr, functionName);
    default:
        throw BinderException(
            stringFormat("{} is a {}. Scalar function, aggregate function or macro was expected. ",
                functionName, CatalogEntryTypeUtils::toString(entry->getType())));
    }
}

std::shared_ptr<Expression> ExpressionBinder::bindScalarFunctionExpression(
    const ParsedExpression& parsedExpression, const std::string& functionName) {
    expression_vector children;
    for (auto i = 0u; i < parsedExpression.getNumChildren(); ++i) {
        auto expr = bindExpression(*parsedExpression.getChild(i));
        if (parsedExpression.getChild(i)->hasAlias()) {
            expr->setAlias(parsedExpression.getChild(i)->getAlias());
        }
        children.push_back(expr);
    }
    return bindScalarFunctionExpression(children, functionName);
}

static std::vector<LogicalType> getTypes(const expression_vector& exprs) {
    std::vector<LogicalType> result;
    for (auto& expr : exprs) {
        result.push_back(expr->getDataType().copy());
    }
    return result;
}

std::shared_ptr<Expression> ExpressionBinder::bindScalarFunctionExpression(
    const expression_vector& children, const std::string& functionName) {
    auto catalog = context->getCatalog();
    auto transaction = context->getTransaction();
    auto childrenTypes = getTypes(children);

    auto entry = catalog->getFunctionEntry(transaction, functionName);

    auto function = BuiltInFunctionsUtils::matchFunction(functionName, childrenTypes,
        entry->ptrCast<FunctionCatalogEntry>())
                        ->ptrCast<ScalarFunction>()
                        ->copy();
    if (children.size() == 2 && children[1]->expressionType == ExpressionType::LAMBDA) {
        if (!function->isListLambda) {
            throw BinderException(stringFormat("{} does not support lambda input.", functionName));
        }
        bindLambdaExpression(*children[0], *children[1]);
    }
    expression_vector childrenAfterCast;
    std::unique_ptr<function::FunctionBindData> bindData;
    if (functionName == CastAnyFunction::name) {
        bindData = function->bindFunc({children, function.get(), context});
        if (bindData == nullptr) { // No need to cast.
            // TODO(Xiyang): We should return a deep copy otherwise the same expression might
            // appear in the final projection list repeatedly.
            // E.g. RETURN cast([NULL], "INT64[1][]"), cast([NULL], "INT64[1][][]")
            return children[0];
        }
        auto childAfterCast = children[0];
        if (children[0]->getDataType().getLogicalTypeID() == LogicalTypeID::ANY) {
            childAfterCast = implicitCastIfNecessary(children[0], LogicalType::STRING());
        }
        childrenAfterCast.push_back(std::move(childAfterCast));
    } else {
        if (function->bindFunc) {
            bindData = function->bindFunc({children, function.get(), context});
        } else {
            bindData = std::make_unique<FunctionBindData>(LogicalType(function->returnTypeID));
        }
        if (!bindData->paramTypes.empty()) {
            for (auto i = 0u; i < children.size(); ++i) {
                childrenAfterCast.push_back(
                    implicitCastIfNecessary(children[i], bindData->paramTypes[i]));
            }
        } else {
            for (auto i = 0u; i < children.size(); ++i) {
                auto id = function->isVarLength ? function->parameterTypeIDs[0] :
                                                  function->parameterTypeIDs[i];
                auto type = LogicalType(id);
                childrenAfterCast.push_back(implicitCastIfNecessary(children[i], type));
            }
        }
    }
    auto uniqueExpressionName =
        ScalarFunctionExpression::getUniqueName(function->name, childrenAfterCast);
    return std::make_shared<ScalarFunctionExpression>(ExpressionType::FUNCTION, std::move(function),
        std::move(bindData), std::move(childrenAfterCast), uniqueExpressionName);
}

std::shared_ptr<Expression> ExpressionBinder::bindRewriteFunctionExpression(
    const parser::ParsedExpression& expr) {
    auto& funcExpr = expr.constCast<ParsedFunctionExpression>();
    expression_vector children;
    for (auto i = 0u; i < expr.getNumChildren(); ++i) {
        children.push_back(bindExpression(*expr.getChild(i)));
    }
    auto childrenTypes = getTypes(children);
    auto functionName = funcExpr.getNormalizedFunctionName();
    auto entry = context->getCatalog()->getFunctionEntry(context->getTransaction(), functionName);
    auto match = BuiltInFunctionsUtils::matchFunction(functionName, childrenTypes,
        entry->ptrCast<FunctionCatalogEntry>());
    auto function = match->constPtrCast<RewriteFunction>();
    KU_ASSERT(function->rewriteFunc != nullptr);
    return function->rewriteFunc(children, this);
}

std::shared_ptr<Expression> ExpressionBinder::bindAggregateFunctionExpression(
    const ParsedExpression& parsedExpression, const std::string& functionName, bool isDistinct) {
    std::vector<LogicalType> childrenTypes;
    expression_vector children;
    for (auto i = 0u; i < parsedExpression.getNumChildren(); ++i) {
        auto child = bindExpression(*parsedExpression.getChild(i));
        childrenTypes.push_back(child->dataType.copy());
        children.push_back(std::move(child));
    }
    auto entry = context->getCatalog()->getFunctionEntry(context->getTransaction(), functionName);
    auto function = BuiltInFunctionsUtils::matchAggregateFunction(functionName, childrenTypes,
        isDistinct, entry->ptrCast<FunctionCatalogEntry>())
                        ->copy();
    if (function.paramRewriteFunc) {
        function.paramRewriteFunc(children);
    }
    if (functionName == CollectFunction::name && parsedExpression.hasAlias() &&
        children[0]->getDataType().getLogicalTypeID() == LogicalTypeID::NODE) {
        auto& node = children[0]->constCast<NodeExpression>();
        binder->scope.memorizeTableEntries(parsedExpression.getAlias(), node.getEntries());
    }
    auto uniqueExpressionName =
        AggregateFunctionExpression::getUniqueName(function.name, children, function.isDistinct);
    if (children.empty()) {
        uniqueExpressionName = binder->getUniqueExpressionName(uniqueExpressionName);
    }
    std::unique_ptr<FunctionBindData> bindData;
    if (function.bindFunc) {
        bindData = function.bindFunc({children, &function, context});
    } else {
        bindData = std::make_unique<function::FunctionBindData>(LogicalType(function.returnTypeID));
    }
    return std::make_shared<AggregateFunctionExpression>(std::move(function), std::move(bindData),
        std::move(children), uniqueExpressionName);
}

std::shared_ptr<Expression> ExpressionBinder::bindMacroExpression(
    const ParsedExpression& parsedExpression, const std::string& macroName) {
    auto scalarMacroFunction =
        context->getCatalog()->getScalarMacroFunction(context->getTransaction(), macroName);
    auto macroExpr = scalarMacroFunction->expression->copy();
    auto parameterVals = scalarMacroFunction->getDefaultParameterVals();
    auto& parsedFuncExpr = parsedExpression.constCast<ParsedFunctionExpression>();
    auto positionalArgs = scalarMacroFunction->getPositionalArgs();
    if (parsedFuncExpr.getNumChildren() > scalarMacroFunction->getNumArgs() ||
        parsedFuncExpr.getNumChildren() < positionalArgs.size()) {
        throw BinderException{"Invalid number of arguments for macro " + macroName + "."};
    }
    // Bind positional arguments.
    for (auto i = 0u; i < positionalArgs.size(); i++) {
        parameterVals[positionalArgs[i]] = parsedFuncExpr.getChild(i);
    }
    // Bind arguments with default values.
    for (auto i = positionalArgs.size(); i < parsedFuncExpr.getNumChildren(); i++) {
        auto parameterName =
            scalarMacroFunction->getDefaultParameterName(i - positionalArgs.size());
        parameterVals[parameterName] = parsedFuncExpr.getChild(i);
    }
    auto macroParameterReplacer = std::make_unique<MacroParameterReplacer>(parameterVals);
    return bindExpression(*macroParameterReplacer->visit(std::move(macroExpr)));
}

// Function rewriting happens when we need to expose internal property access through function so
// that it becomes read-only or the function involves catalog information. Currently we write
// Before             |        After
// LABEL(a)           |        LIST_EXTRACT(offset(a), [table names from catalog])
// STARTNODE(a)       |        a._src
// ENDNODE(a)         |        a._dst
std::shared_ptr<Expression> ExpressionBinder::rewriteFunctionExpression(
    const parser::ParsedExpression& parsedExpression, const std::string& functionName) {
    if (functionName == LabelFunction::name) {
        auto child = bindExpression(*parsedExpression.getChild(0));
        ExpressionUtil::validateDataType(*child,
            std::vector<LogicalTypeID>{LogicalTypeID::NODE, LogicalTypeID::REL});
        return bindLabelFunction(*child);
    } else if (functionName == StartNodeFunction::name) {
        auto child = bindExpression(*parsedExpression.getChild(0));
        ExpressionUtil::validateDataType(*child, LogicalTypeID::REL);
        return bindStartNodeExpression(*child);
    } else if (functionName == EndNodeFunction::name) {
        auto child = bindExpression(*parsedExpression.getChild(0));
        ExpressionUtil::validateDataType(*child, LogicalTypeID::REL);
        return bindEndNodeExpression(*child);
    }
    return nullptr;
}

std::shared_ptr<Expression> ExpressionBinder::bindStartNodeExpression(
    const Expression& expression) {
    auto& rel = (RelExpression&)expression;
    return rel.getSrcNode();
}

std::shared_ptr<Expression> ExpressionBinder::bindEndNodeExpression(const Expression& expression) {
    auto& rel = (RelExpression&)expression;
    return rel.getDstNode();
}

static std::vector<std::unique_ptr<Value>> populateLabelValues(
    std::vector<TableCatalogEntry*> entries) {
    std::unordered_map<table_id_t, std::string> map;
    common::table_id_t maxTableID = 0;
    for (auto& entry : entries) {
        map.insert({entry->getTableID(), entry->getName()});
        if (entry->getTableID() > maxTableID) {
            maxTableID = entry->getTableID();
        }
    }
    std::vector<std::unique_ptr<Value>> labels;
    labels.resize(maxTableID + 1);
    for (auto i = 0u; i < labels.size(); ++i) {
        if (map.contains(i)) {
            labels[i] = std::make_unique<Value>(LogicalType::STRING(), map.at(i));
        } else {
            // TODO(Xiyang/Guodong): change to null literal once we support null in LIST type.
            labels[i] = std::make_unique<Value>(LogicalType::STRING(), std::string(""));
        }
    }
    return labels;
}

std::shared_ptr<Expression> ExpressionBinder::bindLabelFunction(const Expression& expression) {
    auto listType = LogicalType::LIST(LogicalType::STRING());
    expression_vector children;
    switch (expression.getDataType().getLogicalTypeID()) {
    case LogicalTypeID::NODE: {
        auto& node = expression.constCast<NodeExpression>();
        if (node.isEmpty()) {
            return createLiteralExpression("");
        }
        if (!node.isMultiLabeled()) {
            auto labelName = node.getSingleEntry()->getName();
            return createLiteralExpression(Value(LogicalType::STRING(), labelName));
        }
        children.push_back(node.getInternalID());
        auto labelsValue = Value(std::move(listType), populateLabelValues(node.getEntries()));
        children.push_back(createLiteralExpression(labelsValue));
    } break;
    case LogicalTypeID::REL: {
        auto& rel = expression.constCast<RelExpression>();
        if (rel.isEmpty()) {
            return createLiteralExpression("");
        }
        if (!rel.isMultiLabeled()) {
            auto labelName = rel.getSingleEntry()->getName();
            return createLiteralExpression(Value(LogicalType::STRING(), labelName));
        }
        children.push_back(rel.getInternalIDProperty());
        auto labelsValue = Value(std::move(listType), populateLabelValues(rel.getEntries()));
        children.push_back(createLiteralExpression(labelsValue));
    } break;
    default:
        KU_UNREACHABLE;
    }
    auto function = std::make_unique<ScalarFunction>(LabelFunction::name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::INT64},
        LogicalTypeID::STRING, LabelFunction::execFunction);
    auto bindData = std::make_unique<function::FunctionBindData>(LogicalType::STRING());
    auto uniqueExpressionName =
        ScalarFunctionExpression::getUniqueName(LabelFunction::name, children);
    return std::make_shared<ScalarFunctionExpression>(ExpressionType::FUNCTION, std::move(function),
        std::move(bindData), std::move(children), uniqueExpressionName);
}

} // namespace binder
} // namespace kuzu
