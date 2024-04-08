#include "binder/binder.h"
#include "binder/expression/expression_util.h"
#include "binder/expression/function_expression.h"
#include "binder/expression/property_expression.h"
#include "binder/expression_binder.h"
#include "catalog/catalog.h"
#include "common/exception/binder.h"
#include "function/aggregate/collect.h"
#include "function/arithmetic/vector_arithmetic_functions.h"
#include "function/built_in_function_utils.h"
#include "function/cast/vector_cast_functions.h"
#include "function/path/vector_path_functions.h"
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
    auto& funcExpr =
        ku_dynamic_cast<const ParsedExpression&, const ParsedFunctionExpression&>(expr);
    auto functionName = funcExpr.getNormalizedFunctionName();
    auto result = rewriteFunctionExpression(expr, functionName);
    if (result != nullptr) {
        return result;
    }
    auto entry = context->getCatalog()->getFunctionEntry(context->getTx(), functionName);
    switch (entry->getType()) {
    case CatalogEntryType::SCALAR_FUNCTION_ENTRY:
        return bindScalarFunctionExpression(expr, functionName);
    case CatalogEntryType::REWRITE_FUNCTION_ENTRY:
        return bindRewriteFunctionExpression(expr);
    case CatalogEntryType::AGGREGATE_FUNCTION_ENTRY:
        return bindAggregateFunctionExpression(expr, functionName, funcExpr.getIsDistinct());
    case CatalogEntryType::SCALAR_MACRO_ENTRY:
        return bindMacroExpression(expr, functionName);
    default:
        KU_UNREACHABLE;
    }
}

std::shared_ptr<Expression> ExpressionBinder::bindScalarFunctionExpression(
    const ParsedExpression& parsedExpression, const std::string& functionName) {
    expression_vector children;
    for (auto i = 0u; i < parsedExpression.getNumChildren(); ++i) {
        auto child = bindExpression(*parsedExpression.getChild(i));
        children.push_back(std::move(child));
    }
    return bindScalarFunctionExpression(children, functionName);
}

static std::vector<LogicalType> getTypes(const expression_vector& exprs) {
    std::vector<LogicalType> result;
    for (auto& expr : exprs) {
        result.push_back(expr->getDataType());
    }
    return result;
}

std::shared_ptr<Expression> ExpressionBinder::bindScalarFunctionExpression(
    const expression_vector& children, const std::string& functionName) {
    auto childrenTypes = getTypes(children);
    auto functions = context->getCatalog()->getFunctions(context->getTx());
    auto function = ku_dynamic_cast<Function*, function::ScalarFunction*>(
        function::BuiltInFunctionsUtils::matchFunction(functionName, childrenTypes, functions));
    expression_vector childrenAfterCast;
    std::unique_ptr<function::FunctionBindData> bindData;
    if (functionName == CastAnyFunction::name) {
        bindData = function->bindFunc(children, function);
        if (bindData == nullptr) {
            return children[0];
        }
        auto childAfterCast = children[0];
        // See castBindFunc for explanation.
        if (children[0]->getDataType().getLogicalTypeID() == LogicalTypeID::ANY) {
            childAfterCast = implicitCastIfNecessary(children[0], LogicalTypeID::STRING);
        }
        childrenAfterCast.push_back(std::move(childAfterCast));
    } else {
        for (auto i = 0u; i < children.size(); ++i) {
            auto targetType = function->isVarLength ? function->parameterTypeIDs[0] :
                                                      function->parameterTypeIDs[i];
            childrenAfterCast.push_back(implicitCastIfNecessary(children[i], targetType));
        }
        if (function->bindFunc) {
            bindData = function->bindFunc(childrenAfterCast, function);
        } else {
            bindData = std::make_unique<function::FunctionBindData>(
                std::make_unique<LogicalType>(function->returnTypeID));
        }
    }
    auto uniqueExpressionName =
        ScalarFunctionExpression::getUniqueName(function->name, childrenAfterCast);
    return make_shared<ScalarFunctionExpression>(functionName, ExpressionType::FUNCTION,
        std::move(bindData), std::move(childrenAfterCast), function->execFunc, function->selectFunc,
        function->compileFunc, uniqueExpressionName);
}

std::shared_ptr<Expression> ExpressionBinder::bindRewriteFunctionExpression(
    const parser::ParsedExpression& expr) {
    auto& funcExpr =
        ku_dynamic_cast<const ParsedExpression&, const ParsedFunctionExpression&>(expr);
    expression_vector children;
    for (auto i = 0u; i < expr.getNumChildren(); ++i) {
        children.push_back(bindExpression(*expr.getChild(i)));
    }
    auto childrenTypes = getTypes(children);
    auto functions = context->getCatalog()->getFunctions(context->getTx());
    auto match = BuiltInFunctionsUtils::matchFunction(funcExpr.getNormalizedFunctionName(),
        childrenTypes, functions);
    auto function = ku_dynamic_cast<Function*, RewriteFunction*>(match);
    KU_ASSERT(function->rewriteFunc != nullptr);
    return function->rewriteFunc(children, this);
}

std::shared_ptr<Expression> ExpressionBinder::bindAggregateFunctionExpression(
    const ParsedExpression& parsedExpression, const std::string& functionName, bool isDistinct) {
    std::vector<LogicalType> childrenTypes;
    expression_vector children;
    for (auto i = 0u; i < parsedExpression.getNumChildren(); ++i) {
        auto child = bindExpression(*parsedExpression.getChild(i));
        auto childTypeID = child->dataType.getLogicalTypeID();
        if (isDistinct &&
            (childTypeID == LogicalTypeID::NODE || childTypeID == LogicalTypeID::REL)) {
            throw BinderException{"DISTINCT is not supported for NODE or REL type."};
        }
        childrenTypes.push_back(child->dataType);
        children.push_back(std::move(child));
    }
    auto functions = context->getCatalog()->getFunctions(context->getTx());
    auto function = function::BuiltInFunctionsUtils::matchAggregateFunction(functionName,
        childrenTypes, isDistinct, functions)
                        ->clone();
    if (function->paramRewriteFunc) {
        function->paramRewriteFunc(children);
    }
    if (functionName == CollectFunction::name && parsedExpression.hasAlias() &&
        children[0]->getDataType().getLogicalTypeID() == LogicalTypeID::NODE) {
        auto node = ku_dynamic_cast<Expression*, NodeExpression*>(children[0].get());
        binder->scope.memorizeTableIDs(parsedExpression.getAlias(), node->getTableIDs());
    }
    auto uniqueExpressionName =
        AggregateFunctionExpression::getUniqueName(function->name, children, function->isDistinct);
    if (children.empty()) {
        uniqueExpressionName = binder->getUniqueExpressionName(uniqueExpressionName);
    }
    std::unique_ptr<function::FunctionBindData> bindData;
    if (function->bindFunc) {
        bindData = function->bindFunc(children, function.get());
    } else {
        bindData = std::make_unique<function::FunctionBindData>(
            std::make_unique<LogicalType>(function->returnTypeID));
    }
    return make_shared<AggregateFunctionExpression>(functionName, std::move(bindData),
        std::move(children), std::move(function), uniqueExpressionName);
}

std::shared_ptr<Expression> ExpressionBinder::bindMacroExpression(
    const ParsedExpression& parsedExpression, const std::string& macroName) {
    auto scalarMacroFunction = context->getCatalog()->getScalarMacroFunction(macroName);
    auto macroExpr = scalarMacroFunction->expression->copy();
    auto parameterVals = scalarMacroFunction->getDefaultParameterVals();
    auto& parsedFuncExpr =
        ku_dynamic_cast<const ParsedExpression&, const ParsedFunctionExpression&>(parsedExpression);
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
// LENGTH(e)          |        e._length
// STARTNODE(a)       |        a._src
// ENDNODE(a)         |        a._dst
std::shared_ptr<Expression> ExpressionBinder::rewriteFunctionExpression(
    const parser::ParsedExpression& parsedExpression, const std::string& functionName) {
    if (functionName == LabelFunction::name) {
        auto child = bindExpression(*parsedExpression.getChild(0));
        validateExpectedDataType(*child,
            std::vector<LogicalTypeID>{LogicalTypeID::NODE, LogicalTypeID::REL});
        return bindLabelFunction(*child);
    } else if (functionName == LengthFunction::name) {
        auto child = bindExpression(*parsedExpression.getChild(0));
        return bindRecursiveJoinLengthFunction(*child);
    } else if (functionName == StartNodeFunction::name) {
        auto child = bindExpression(*parsedExpression.getChild(0));
        validateExpectedDataType(*child, std::vector<LogicalTypeID>{LogicalTypeID::REL});
        return bindStartNodeExpression(*child);
    } else if (functionName == EndNodeFunction::name) {
        auto child = bindExpression(*parsedExpression.getChild(0));
        validateExpectedDataType(*child, std::vector<LogicalTypeID>{LogicalTypeID::REL});
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

static std::vector<std::unique_ptr<Value>> populateLabelValues(std::vector<table_id_t> tableIDs,
    const catalog::Catalog& catalog, transaction::Transaction* tx) {
    auto tableIDsSet = std::unordered_set<table_id_t>(tableIDs.begin(), tableIDs.end());
    table_id_t maxTableID = *std::max_element(tableIDsSet.begin(), tableIDsSet.end());
    std::vector<std::unique_ptr<Value>> labels;
    labels.resize(maxTableID + 1);
    for (auto i = 0u; i < labels.size(); ++i) {
        if (tableIDsSet.contains(i)) {
            labels[i] = std::make_unique<Value>(LogicalType::STRING(), catalog.getTableName(tx, i));
        } else {
            // TODO(Xiyang/Guodong): change to null literal once we support null in LIST type.
            labels[i] = std::make_unique<Value>(LogicalType::STRING(), std::string(""));
        }
    }
    return labels;
}

std::shared_ptr<Expression> ExpressionBinder::bindLabelFunction(const Expression& expression) {
    auto catalog = context->getCatalog();
    auto listType = LogicalType::LIST(LogicalType::STRING());
    expression_vector children;
    switch (expression.getDataType().getLogicalTypeID()) {
    case LogicalTypeID::NODE: {
        auto& node = (NodeExpression&)expression;
        if (!node.isMultiLabeled()) {
            auto labelName = catalog->getTableName(context->getTx(), node.getSingleTableID());
            return createLiteralExpression(
                std::make_unique<Value>(LogicalType::STRING(), labelName));
        }
        auto nodeTableIDs = catalog->getNodeTableIDs(context->getTx());
        children.push_back(node.getInternalID());
        auto labelsValue = std::make_unique<Value>(std::move(listType),
            populateLabelValues(nodeTableIDs, *catalog, context->getTx()));
        children.push_back(createLiteralExpression(std::move(labelsValue)));
    } break;
    case LogicalTypeID::REL: {
        auto& rel = (RelExpression&)expression;
        if (!rel.isMultiLabeled()) {
            auto labelName = catalog->getTableName(context->getTx(), rel.getSingleTableID());
            return createLiteralExpression(
                std::make_unique<Value>(LogicalType::STRING(), labelName));
        }
        auto relTableIDs = catalog->getRelTableIDs(context->getTx());
        children.push_back(rel.getInternalIDProperty());
        auto labelsValue = std::make_unique<Value>(std::move(listType),
            populateLabelValues(relTableIDs, *catalog, context->getTx()));
        children.push_back(createLiteralExpression(std::move(labelsValue)));
    } break;
    default:
        KU_UNREACHABLE;
    }
    auto execFunc = function::LabelFunction::execFunction;
    auto bindData = std::make_unique<function::FunctionBindData>(LogicalType::STRING());
    auto uniqueExpressionName =
        ScalarFunctionExpression::getUniqueName(LabelFunction::name, children);
    return std::make_shared<ScalarFunctionExpression>(LabelFunction::name, ExpressionType::FUNCTION,
        std::move(bindData), std::move(children), execFunc, nullptr, uniqueExpressionName);
}

std::unique_ptr<Expression> ExpressionBinder::createInternalLengthExpression(
    const Expression& expression) {
    auto& rel = (RelExpression&)expression;
    std::unordered_map<table_id_t, property_id_t> propertyIDPerTable;
    for (auto tableID : rel.getTableIDs()) {
        propertyIDPerTable.insert({tableID, INVALID_PROPERTY_ID});
    }
    return std::make_unique<PropertyExpression>(LogicalType(LogicalTypeID::INT64),
        InternalKeyword::LENGTH, rel.getUniqueName(), rel.getVariableName(),
        std::move(propertyIDPerTable), false /* isPrimaryKey */);
}

std::shared_ptr<Expression> ExpressionBinder::bindRecursiveJoinLengthFunction(
    const Expression& expression) {
    if (expression.getDataType().getLogicalTypeID() != LogicalTypeID::RECURSIVE_REL) {
        return nullptr;
    }
    if (expression.expressionType == common::ExpressionType::PATH) {
        int64_t numRels = 0u;
        expression_vector recursiveRels;
        for (auto& child : expression.getChildren()) {
            if (ExpressionUtil::isRelPattern(*child)) {
                numRels++;
            } else if (ExpressionUtil::isRecursiveRelPattern(*child)) {
                recursiveRels.push_back(child);
            }
        }
        auto numRelsExpression = createLiteralExpression(std::make_unique<Value>(numRels));
        if (recursiveRels.empty()) {
            return numRelsExpression;
        }
        expression_vector children;
        children.push_back(std::move(numRelsExpression));
        children.push_back(
            ku_dynamic_cast<Expression&, RelExpression&>(*recursiveRels[0]).getLengthExpression());
        auto result = bindScalarFunctionExpression(children, AddFunction::name);
        for (auto i = 1u; i < recursiveRels.size(); ++i) {
            children[0] = std::move(result);
            children[1] = ku_dynamic_cast<Expression&, RelExpression&>(*recursiveRels[i])
                              .getLengthExpression();
            result = bindScalarFunctionExpression(children, AddFunction::name);
        }
        return result;
    } else if (ExpressionUtil::isRecursiveRelPattern(expression)) {
        auto& recursiveRel = ku_dynamic_cast<const Expression&, const RelExpression&>(expression);
        return recursiveRel.getLengthExpression();
    }
    return nullptr;
}

} // namespace binder
} // namespace kuzu
