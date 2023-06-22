#include "binder/binder.h"
#include "binder/expression/function_expression.h"
#include "binder/expression/literal_expression.h"
#include "binder/expression_binder.h"
#include "common/string_utils.h"
#include "function/schema/vector_label_operations.h"
#include "parser/expression/parsed_function_expression.h"

using namespace kuzu::common;
using namespace kuzu::parser;

namespace kuzu {
namespace binder {

std::shared_ptr<Expression> ExpressionBinder::bindFunctionExpression(
    const ParsedExpression& parsedExpression) {
    auto& parsedFunctionExpression = (ParsedFunctionExpression&)parsedExpression;
    auto functionName = parsedFunctionExpression.getFunctionName();
    StringUtils::toUpper(functionName);
    auto result = rewriteFunctionExpression(parsedExpression, functionName);
    if (result != nullptr) {
        return result;
    }
    auto functionType = binder->catalog.getFunctionType(functionName);
    if (functionType == FUNCTION) {
        return bindScalarFunctionExpression(parsedExpression, functionName);
    } else {
        assert(functionType == AGGREGATE_FUNCTION);
        return bindAggregateFunctionExpression(
            parsedExpression, functionName, parsedFunctionExpression.getIsDistinct());
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

std::shared_ptr<Expression> ExpressionBinder::bindScalarFunctionExpression(
    const expression_vector& children, const std::string& functionName) {
    auto builtInFunctions = binder->catalog.getBuiltInScalarFunctions();
    std::vector<LogicalType> childrenTypes;
    for (auto& child : children) {
        childrenTypes.push_back(child->dataType);
    }
    auto function = builtInFunctions->matchFunction(functionName, childrenTypes);
    if (builtInFunctions->canApplyStaticEvaluation(functionName, children)) {
        return staticEvaluate(functionName, children);
    }
    expression_vector childrenAfterCast;
    for (auto i = 0u; i < children.size(); ++i) {
        auto targetType =
            function->isVarLength ? function->parameterTypeIDs[0] : function->parameterTypeIDs[i];
        childrenAfterCast.push_back(implicitCastIfNecessary(children[i], targetType));
    }
    std::unique_ptr<function::FunctionBindData> bindData;
    if (function->bindFunc) {
        bindData = function->bindFunc(childrenAfterCast, function);
    } else {
        bindData =
            std::make_unique<function::FunctionBindData>(LogicalType(function->returnTypeID));
    }
    auto uniqueExpressionName =
        ScalarFunctionExpression::getUniqueName(function->name, childrenAfterCast);
    return make_shared<ScalarFunctionExpression>(functionName, FUNCTION, std::move(bindData),
        std::move(childrenAfterCast), function->execFunc, function->selectFunc,
        function->compileFunc, uniqueExpressionName);
}

std::shared_ptr<Expression> ExpressionBinder::bindAggregateFunctionExpression(
    const ParsedExpression& parsedExpression, const std::string& functionName, bool isDistinct) {
    auto builtInFunctions = binder->catalog.getBuiltInAggregateFunction();
    std::vector<LogicalType> childrenTypes;
    expression_vector children;
    for (auto i = 0u; i < parsedExpression.getNumChildren(); ++i) {
        auto child = bindExpression(*parsedExpression.getChild(i));
        // rewrite aggregate on node or rel as aggregate on their internal IDs.
        // e.g. COUNT(a) -> COUNT(a._id)
        if (child->dataType.getLogicalTypeID() == LogicalTypeID::NODE ||
            child->dataType.getLogicalTypeID() == LogicalTypeID::REL) {
            child = bindInternalIDExpression(child);
        }
        childrenTypes.push_back(child->dataType);
        children.push_back(std::move(child));
    }
    auto function = builtInFunctions->matchFunction(functionName, childrenTypes, isDistinct);
    auto uniqueExpressionName =
        AggregateFunctionExpression::getUniqueName(function->name, children, function->isDistinct);
    if (children.empty()) {
        uniqueExpressionName = binder->getUniqueExpressionName(uniqueExpressionName);
    }
    std::unique_ptr<function::FunctionBindData> bindData;
    if (function->bindFunc) {
        bindData = function->bindFunc(children, function);
    } else {
        bindData =
            std::make_unique<function::FunctionBindData>(LogicalType(function->returnTypeID));
    }
    return make_shared<AggregateFunctionExpression>(functionName, std::move(bindData),
        std::move(children), function->aggregateFunction->clone(), uniqueExpressionName);
}

std::shared_ptr<Expression> ExpressionBinder::staticEvaluate(
    const std::string& functionName, const expression_vector& children) {
    assert(children[0]->expressionType == common::LITERAL);
    auto strVal = ((LiteralExpression*)children[0].get())->getValue()->getValue<std::string>();
    std::unique_ptr<Value> value;
    if (functionName == CAST_TO_DATE_FUNC_NAME) {
        value = std::make_unique<Value>(Date::FromCString(strVal.c_str(), strVal.length()));
    } else if (functionName == CAST_TO_TIMESTAMP_FUNC_NAME) {
        value = std::make_unique<Value>(Timestamp::FromCString(strVal.c_str(), strVal.length()));
    } else {
        assert(functionName == CAST_TO_INTERVAL_FUNC_NAME);
        value = std::make_unique<Value>(Interval::FromCString(strVal.c_str(), strVal.length()));
    }
    return createLiteralExpression(std::move(value));
}

// Function rewriting happens when we need to expose internal property access through function so
// that it becomes read-only or the function involves catalog information. Currently we write
// Before             |        After
// ID(a)              |        a._id
// LABEL(a)           |        LIST_EXTRACT(offset(a), [table names from catalog])
// LENGTH(e)          |        e._length
std::shared_ptr<Expression> ExpressionBinder::rewriteFunctionExpression(
    const parser::ParsedExpression& parsedExpression, const std::string& functionName) {
    if (functionName == ID_FUNC_NAME) {
        auto child = bindExpression(*parsedExpression.getChild(0));
        validateExpectedDataType(*child, std::vector<LogicalTypeID>{LogicalTypeID::NODE,
                                             LogicalTypeID::REL, LogicalTypeID::STRUCT});
        return bindInternalIDExpression(child);
    } else if (functionName == LABEL_FUNC_NAME) {
        auto child = bindExpression(*parsedExpression.getChild(0));
        validateExpectedDataType(
            *child, std::vector<LogicalTypeID>{LogicalTypeID::NODE, LogicalTypeID::REL});
        return bindLabelFunction(*child);
    } else if (functionName == LENGTH_FUNC_NAME) {
        auto child = bindExpression(*parsedExpression.getChild(0));
        return bindRecursiveJoinLengthFunction(*child);
    }
    return nullptr;
}

std::unique_ptr<Expression> ExpressionBinder::createInternalNodeIDExpression(
    const Expression& expression) {
    auto& node = (NodeExpression&)expression;
    std::unordered_map<table_id_t, property_id_t> propertyIDPerTable;
    for (auto tableID : node.getTableIDs()) {
        propertyIDPerTable.insert({tableID, INVALID_PROPERTY_ID});
    }
    return std::make_unique<PropertyExpression>(LogicalType(LogicalTypeID::INTERNAL_ID),
        InternalKeyword::ID, node, std::move(propertyIDPerTable), false /* isPrimaryKey */);
}

std::shared_ptr<Expression> ExpressionBinder::bindInternalIDExpression(
    std::shared_ptr<Expression> expression) {
    switch (expression->getDataType().getLogicalTypeID()) {
    case common::LogicalTypeID::NODE: {
        auto& node = (NodeExpression&)*expression;
        return node.getInternalIDProperty();
    }
    case common::LogicalTypeID::REL: {
        return bindRelPropertyExpression(*expression, InternalKeyword::ID);
    }
    case common::LogicalTypeID::STRUCT: {
        auto stringValue =
            std::make_unique<Value>(LogicalType{LogicalTypeID::STRING}, InternalKeyword::ID);
        return bindScalarFunctionExpression(
            expression_vector{expression, createLiteralExpression(std::move(stringValue))},
            STRUCT_EXTRACT_FUNC_NAME);
    }
    default:
        throw NotImplementedException("ExpressionBinder::bindInternalIDExpression");
    }
}

static std::vector<std::unique_ptr<Value>> populateLabelValues(
    std::vector<table_id_t> tableIDs, const catalog::CatalogContent& catalogContent) {
    auto tableIDsSet = std::unordered_set<table_id_t>(tableIDs.begin(), tableIDs.end());
    table_id_t maxTableID = *std::max_element(tableIDsSet.begin(), tableIDsSet.end());
    std::vector<std::unique_ptr<Value>> labels;
    labels.resize(maxTableID + 1);
    for (auto i = 0; i < labels.size(); ++i) {
        if (tableIDsSet.contains(i)) {
            labels[i] = std::make_unique<Value>(
                LogicalType{LogicalTypeID::STRING}, catalogContent.getTableName(i));
        } else {
            // TODO(Xiyang/Guodong): change to null literal once we support null in LIST type.
            labels[i] =
                std::make_unique<Value>(LogicalType{LogicalTypeID::STRING}, std::string(""));
        }
    }
    return labels;
}

std::shared_ptr<Expression> ExpressionBinder::bindLabelFunction(const Expression& expression) {
    auto catalogContent = binder->catalog.getReadOnlyVersion();
    auto varListTypeInfo =
        std::make_unique<VarListTypeInfo>(std::make_unique<LogicalType>(LogicalTypeID::STRING));
    auto listType =
        std::make_unique<LogicalType>(LogicalTypeID::VAR_LIST, std::move(varListTypeInfo));
    expression_vector children;
    switch (expression.getDataType().getLogicalTypeID()) {
    case common::LogicalTypeID::NODE: {
        auto& node = (NodeExpression&)expression;
        if (!node.isMultiLabeled()) {
            auto labelName = catalogContent->getTableName(node.getSingleTableID());
            return createLiteralExpression(
                std::make_unique<Value>(LogicalType{LogicalTypeID::STRING}, labelName));
        }
        auto nodeTableIDs = catalogContent->getNodeTableIDs();
        children.push_back(node.getInternalIDProperty());
        auto labelsValue =
            std::make_unique<Value>(*listType, populateLabelValues(nodeTableIDs, *catalogContent));
        children.push_back(createLiteralExpression(std::move(labelsValue)));
    } break;
    case common::LogicalTypeID::REL: {
        auto& rel = (RelExpression&)expression;
        if (!rel.isMultiLabeled()) {
            auto labelName = catalogContent->getTableName(rel.getSingleTableID());
            return createLiteralExpression(
                std::make_unique<Value>(LogicalType{LogicalTypeID::STRING}, labelName));
        }
        auto relTableIDs = catalogContent->getRelTableIDs();
        children.push_back(rel.getInternalIDProperty());
        auto labelsValue =
            std::make_unique<Value>(*listType, populateLabelValues(relTableIDs, *catalogContent));
        children.push_back(createLiteralExpression(std::move(labelsValue)));
    } break;
    default:
        throw NotImplementedException("ExpressionBinder::bindLabelFunction");
    }
    auto execFunc = function::LabelVectorOperation::execFunction;
    auto bindData =
        std::make_unique<function::FunctionBindData>(LogicalType(LogicalTypeID::STRING));
    auto uniqueExpressionName = ScalarFunctionExpression::getUniqueName(LABEL_FUNC_NAME, children);
    return make_shared<ScalarFunctionExpression>(LABEL_FUNC_NAME, FUNCTION, std::move(bindData),
        std::move(children), execFunc, nullptr, uniqueExpressionName);
}

std::unique_ptr<Expression> ExpressionBinder::createInternalLengthExpression(
    const Expression& expression) {
    auto& rel = (RelExpression&)expression;
    std::unordered_map<table_id_t, property_id_t> propertyIDPerTable;
    for (auto tableID : rel.getTableIDs()) {
        propertyIDPerTable.insert({tableID, INVALID_PROPERTY_ID});
    }
    return std::make_unique<PropertyExpression>(LogicalType(common::LogicalTypeID::INT64),
        InternalKeyword::LENGTH, rel, std::move(propertyIDPerTable), false /* isPrimaryKey */);
}

std::shared_ptr<Expression> ExpressionBinder::bindRecursiveJoinLengthFunction(
    const Expression& expression) {
    if (expression.getDataType().getLogicalTypeID() != common::LogicalTypeID::RECURSIVE_REL) {
        return nullptr;
    }
    return ((RelExpression&)expression).getLengthExpression();
}

} // namespace binder
} // namespace kuzu
