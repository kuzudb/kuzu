#include "binder/binder.h"
#include "binder/expression/function_expression.h"
#include "binder/expression/literal_expression.h"
#include "binder/expression_binder.h"
#include "function/node/vector_node_operations.h"
#include "parser/expression/parsed_function_expression.h"

namespace kuzu {
namespace binder {

shared_ptr<Expression> ExpressionBinder::bindFunctionExpression(
    const ParsedExpression& parsedExpression) {
    auto& parsedFunctionExpression = (ParsedFunctionExpression&)parsedExpression;
    auto functionName = parsedFunctionExpression.getFunctionName();
    StringUtils::toUpper(functionName);
    // check for special function binding
    if (functionName == ID_FUNC_NAME) {
        return bindInternalIDExpression(parsedExpression);
    } else if (functionName == LABEL_FUNC_NAME) {
        return bindLabelFunction(parsedExpression);
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

shared_ptr<Expression> ExpressionBinder::bindScalarFunctionExpression(
    const ParsedExpression& parsedExpression, const string& functionName) {
    auto builtInFunctions = binder->catalog.getBuiltInScalarFunctions();
    vector<DataType> childrenTypes;
    expression_vector children;
    for (auto i = 0u; i < parsedExpression.getNumChildren(); ++i) {
        auto child = bindExpression(*parsedExpression.getChild(i));
        childrenTypes.push_back(child->dataType);
        children.push_back(std::move(child));
    }
    auto function = builtInFunctions->matchFunction(functionName, childrenTypes);
    if (builtInFunctions->canApplyStaticEvaluation(functionName, children)) {
        return staticEvaluate(functionName, parsedExpression, children);
    }
    expression_vector childrenAfterCast;
    for (auto i = 0u; i < children.size(); ++i) {
        auto targetType =
            function->isVarLength ? function->parameterTypeIDs[0] : function->parameterTypeIDs[i];
        childrenAfterCast.push_back(implicitCastIfNecessary(children[i], targetType));
    }
    DataType returnType;
    if (function->bindFunc) {
        function->bindFunc(childrenTypes, function, returnType);
    } else {
        returnType = DataType(function->returnTypeID);
    }
    auto uniqueExpressionName =
        ScalarFunctionExpression::getUniqueName(function->name, childrenAfterCast);
    return make_shared<ScalarFunctionExpression>(FUNCTION, returnType, std::move(childrenAfterCast),
        function->execFunc, function->selectFunc, uniqueExpressionName);
}

shared_ptr<Expression> ExpressionBinder::bindAggregateFunctionExpression(
    const ParsedExpression& parsedExpression, const string& functionName, bool isDistinct) {
    auto builtInFunctions = binder->catalog.getBuiltInAggregateFunction();
    vector<DataType> childrenTypes;
    expression_vector children;
    for (auto i = 0u; i < parsedExpression.getNumChildren(); ++i) {
        auto child = bindExpression(*parsedExpression.getChild(i));
        // rewrite aggregate on node or rel as aggregate on their internal IDs.
        // e.g. COUNT(a) -> COUNT(a._id)
        if (child->dataType.typeID == NODE || child->dataType.typeID == REL) {
            child = bindInternalIDExpression(*child);
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
    return make_shared<AggregateFunctionExpression>(DataType(function->returnTypeID),
        std::move(children), function->aggregateFunction->clone(), uniqueExpressionName);
}

shared_ptr<Expression> ExpressionBinder::staticEvaluate(const string& functionName,
    const ParsedExpression& parsedExpression, const expression_vector& children) {
    assert(children[0]->expressionType == common::LITERAL);
    auto strVal = ((LiteralExpression*)children[0].get())->getValue()->getValue<string>();
    if (functionName == CAST_TO_DATE_FUNC_NAME) {
        return make_shared<LiteralExpression>(
            make_unique<Value>(Date::FromCString(strVal.c_str(), strVal.length())));
    } else if (functionName == CAST_TO_TIMESTAMP_FUNC_NAME) {
        return make_shared<LiteralExpression>(
            make_unique<Value>(Timestamp::FromCString(strVal.c_str(), strVal.length())));
    } else {
        assert(functionName == CAST_TO_INTERVAL_FUNC_NAME);
        return make_shared<LiteralExpression>(
            make_unique<Value>(Interval::FromCString(strVal.c_str(), strVal.length())));
    }
}

shared_ptr<Expression> ExpressionBinder::bindInternalIDExpression(
    const ParsedExpression& parsedExpression) {
    auto child = bindExpression(*parsedExpression.getChild(0));
    validateExpectedDataType(*child, unordered_set<DataTypeID>{NODE, REL});
    return bindInternalIDExpression(*child);
}

shared_ptr<Expression> ExpressionBinder::bindInternalIDExpression(const Expression& expression) {
    if (expression.dataType.typeID == NODE) {
        auto& node = (NodeExpression&)expression;
        return node.getInternalIDProperty();
    } else {
        assert(expression.dataType.typeID == REL);
        return bindRelPropertyExpression(expression, INTERNAL_ID_SUFFIX);
    }
}

unique_ptr<Expression> ExpressionBinder::createInternalNodeIDExpression(
    const Expression& expression) {
    auto& node = (NodeExpression&)expression;
    unordered_map<table_id_t, property_id_t> propertyIDPerTable;
    for (auto tableID : node.getTableIDs()) {
        propertyIDPerTable.insert({tableID, INVALID_PROPERTY_ID});
    }
    auto result = make_unique<PropertyExpression>(
        DataType(INTERNAL_ID), INTERNAL_ID_SUFFIX, node, std::move(propertyIDPerTable));
    return result;
}

shared_ptr<Expression> ExpressionBinder::bindLabelFunction(
    const ParsedExpression& parsedExpression) {
    // bind child node
    auto child = bindExpression(*parsedExpression.getChild(0));
    assert(child->dataType.typeID == common::NODE);
    return bindNodeLabelFunction(*child);
}

shared_ptr<Expression> ExpressionBinder::bindNodeLabelFunction(const Expression& expression) {
    auto catalogContent = binder->catalog.getReadOnlyVersion();
    auto& node = (NodeExpression&)expression;
    if (!node.isMultiLabeled()) {
        auto labelName = catalogContent->getTableName(node.getSingleTableID());
        return make_shared<LiteralExpression>(make_unique<Value>(labelName));
    }
    // bind string node labels as list literal
    auto nodeTableIDs = catalogContent->getNodeTableIDs();
    table_id_t maxNodeTableID = *std::max_element(nodeTableIDs.begin(), nodeTableIDs.end());
    vector<unique_ptr<Value>> nodeLabels;
    nodeLabels.resize(maxNodeTableID + 1);
    for (auto i = 0; i < nodeLabels.size(); ++i) {
        if (catalogContent->containNodeTable(i)) {
            nodeLabels[i] = make_unique<Value>(catalogContent->getTableName(i));
        } else {
            // TODO(Xiyang/Guodong): change to null literal once we support null in LIST type.
            nodeLabels[i] = make_unique<Value>(string(""));
        }
    }
    auto literalDataType = DataType(LIST, make_unique<DataType>(STRING));
    expression_vector children;
    children.push_back(node.getInternalIDProperty());
    children.push_back(
        make_shared<LiteralExpression>(make_unique<Value>(literalDataType, std::move(nodeLabels))));
    auto execFunc = NodeLabelVectorOperation::execFunction;
    auto uniqueExpressionName = ScalarFunctionExpression::getUniqueName(LABEL_FUNC_NAME, children);
    return make_shared<ScalarFunctionExpression>(
        FUNCTION, DataType(STRING), std::move(children), execFunc, nullptr, uniqueExpressionName);
}

} // namespace binder
} // namespace kuzu
