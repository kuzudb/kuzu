#include "binder/binder.h"
#include "binder/expression/expression_util.h"
#include "binder/expression/literal_expression.h"
#include "binder/query/reading_clause/bound_gds_call.h"
#include "binder/query/reading_clause/bound_table_function_call.h"
#include "catalog/catalog.h"
#include "common/exception/binder.h"
#include "function/built_in_function_utils.h"
#include "function/gds_function.h"
#include "graph/graph_entry.h"
#include "parser/expression/parsed_function_expression.h"
#include "parser/expression/parsed_property_expression.h"
#include "parser/expression/parsed_variable_expression.h"
#include "parser/query/reading_clause/in_query_call_clause.h"
#include "storage/storage_manager.h"
#include "storage/index/vector_index_header.h"

using namespace kuzu::common;
using namespace kuzu::catalog;
using namespace kuzu::parser;
using namespace kuzu::function;
using namespace kuzu::catalog;
using namespace kuzu::graph;

namespace kuzu {
namespace binder {

std::unique_ptr<BoundReadingClause> Binder::bindInQueryCall(const ReadingClause& readingClause) {
    auto& call = readingClause.constCast<InQueryCallClause>();
    auto expr = call.getFunctionExpression();
    auto functionExpr = expr->constPtrCast<ParsedFunctionExpression>();
    auto functionName = functionExpr->getFunctionName();
    std::unique_ptr<BoundReadingClause> boundReadingClause;
    expression_vector outExprs;
    auto catalogSet = clientContext->getCatalog()->getFunctions(clientContext->getTx());
    auto entry = BuiltInFunctionsUtils::getFunctionCatalogEntry(clientContext->getTx(),
        functionName, catalogSet);
    switch (entry->getType()) {
    case CatalogEntryType::TABLE_FUNCTION_ENTRY: {
        expression_vector children;
        std::vector<LogicalType> childrenTypes;
        for (auto i = 0u; i < functionExpr->getNumChildren(); i++) {
            auto child = expressionBinder.bindExpression(*functionExpr->getChild(i));
            children.push_back(child);
            childrenTypes.push_back(child->getDataType().copy());
        }
        auto func = BuiltInFunctionsUtils::matchFunction(functionName, childrenTypes, entry);
        std::vector<Value> inputValues;
        for (auto& param : children) {
            ExpressionUtil::validateExpressionType(*param, ExpressionType::LITERAL);
            auto literalExpr = param->constPtrCast<LiteralExpression>();
            inputValues.push_back(literalExpr->getValue());
        }
        auto tableFunc = func->constPtrCast<TableFunction>();
        for (auto i = 0u; i < children.size(); ++i) {
            auto parameterTypeID = tableFunc->parameterTypeIDs[i];
            ExpressionUtil::validateDataType(*children[i], parameterTypeID);
        }
        auto bindInput = function::TableFuncBindInput();
        bindInput.inputs = std::move(inputValues);
        auto bindData = tableFunc->bindFunc(clientContext, &bindInput);
        for (auto i = 0u; i < bindData->columnTypes.size(); i++) {
            outExprs.push_back(createVariable(bindData->columnNames[i], bindData->columnTypes[i]));
        }
        auto offset = expressionBinder.createVariableExpression(LogicalType::INT64(),
            std::string(InternalKeyword::ROW_OFFSET));
        boundReadingClause = std::make_unique<BoundTableFunctionCall>(*tableFunc,
            std::move(bindData), std::move(offset), std::move(outExprs));
    } break;
    case CatalogEntryType::GDS_FUNCTION_ENTRY: {
        // The first argument of a GDS function must be a graph variable.
        if (functionExpr->getNumChildren() == 0) {
            throw BinderException(
                stringFormat("{} function requires at least one input", functionName));
        }
        if (functionName == "ANN_SEARCH") {
            boundReadingClause = bindVectorSearchCall(functionName, entry, functionExpr);
        } else {
            GraphEntry graphEntry = GraphEntry(std::vector<table_id_t>(), std::vector<table_id_t>());
            if (functionExpr->getChild(0)->getExpressionType() != ExpressionType::VARIABLE) {
                throw BinderException(
                    stringFormat("First argument of {} function must be a variable", functionName));
            }
            auto varName = functionExpr->getChild(0)
                               ->constPtrCast<ParsedVariableExpression>()
                               ->getVariableName();
            if (!graphEntrySet.hasGraph(varName)) {
                throw BinderException(stringFormat("Cannot find graph {}.", varName));
            }
            graphEntry = graphEntrySet.getEntry(varName);
            expression_vector children;
            std::vector<LogicalType> childrenTypes;
            children.push_back(nullptr); // placeholder for graph variable.
            childrenTypes.push_back(LogicalType::ANY());
            for (auto i = 1u; i < functionExpr->getNumChildren(); i++) {
                auto child = expressionBinder.bindExpression(*functionExpr->getChild(i));
                children.push_back(child);
                childrenTypes.push_back(child->getDataType().copy());
            }
            auto func = BuiltInFunctionsUtils::matchFunction(functionName, childrenTypes, entry);
            auto gdsFunc = *func->constPtrCast<GDSFunction>();
            gdsFunc.gds->bind(children, this, graphEntry);
            outExprs = gdsFunc.gds->getResultColumns(this);
            auto info = BoundGDSCallInfo(gdsFunc.copy(), graphEntry.copy(), std::move(outExprs));
            boundReadingClause = std::make_unique<BoundGDSCall>(std::move(info));
        }
    } break;
    default:
        throw BinderException(
            stringFormat("{} is not a table or algorithm function.", functionName));
    }
    if (call.hasWherePredicate()) {
        auto wherePredicate = bindWhereExpression(*call.getWherePredicate());
        boundReadingClause->setPredicate(std::move(wherePredicate));
    }
    return boundReadingClause;
}

std::unique_ptr<BoundReadingClause> Binder::bindVectorSearchCall(std::string functionName,
    catalog::CatalogEntry* functionCatalogEntry,
    const parser::ParsedFunctionExpression* functionExpr) {
    auto propertyExp = functionExpr->getChild(0)
                           ->constPtrCast<ParsedPropertyExpression>();
    if (propertyExp->getChild(0)->getExpressionType() != ExpressionType::VARIABLE) {
        throw BinderException(
            stringFormat("First argument of {} function must be a variable", functionName));
    }
    auto varName = propertyExp->getChild(0)
                       ->constPtrCast<ParsedVariableExpression>()
                       ->getVariableName();
    //            auto tableId = clientContext->getCatalog()->getTableID(clientContext->getTx(), varName);
    auto nodeExpr = expressionBinder.bindVariableExpression(varName);
    auto node = nodeExpr->constPtrCast<NodeExpression>();
    auto nodeTableIds = node->getTableIDs();
    if (nodeTableIds.size() != 1) {
        throw BinderException(
            stringFormat("First argument of {} function must be a variable that refers to a table", functionName));
    }
    auto tableId = nodeTableIds[0];
    auto nodeTableEntry = clientContext->getCatalog()->getTableCatalogEntry(clientContext->getTx(), tableId);
    auto embeddingPropertyId = nodeTableEntry->getPropertyID(propertyExp->getPropertyName());
    auto header = clientContext->getStorageManager()->getVectorIndexHeaderReadOnlyVersion(tableId, embeddingPropertyId);
    // Considering there's only one partition
    auto partitionHeader = header->getPartitionHeader(0);
    auto relTableName = storage::VectorIndexHeader::getIndexRelTableName(tableId, embeddingPropertyId,
                                                                         partitionHeader->getStartNodeGroupId(),
                                                                         partitionHeader->getEndNodeGroupId());
    auto relTableId = clientContext->getCatalog()->getTableID(clientContext->getTx(), relTableName);
    auto graphEntry = GraphEntry(std::vector<table_id_t>{tableId}, std::vector<table_id_t>{relTableId});

    expression_vector children;
    std::vector<LogicalType> childrenTypes;
    children.push_back(nullptr); // placeholder for graph variable.
    childrenTypes.push_back(LogicalType::ANY());
    children.push_back(nodeExpr);
    childrenTypes.push_back(nodeExpr->getDataType().copy());
    // Add LiteralExpression for embedding property ID.
    children.push_back(std::make_shared<LiteralExpression>(Value((int64_t)embeddingPropertyId), "_embedding_property_id"));
    childrenTypes.push_back(LogicalType::INT64());
    for (auto i = 1u; i < functionExpr->getNumChildren(); i++) {
        auto child = expressionBinder.bindExpression(*functionExpr->getChild(i));
        children.push_back(child);
        childrenTypes.push_back(child->getDataType().copy());
    }
    auto func = BuiltInFunctionsUtils::matchFunction(functionName, childrenTypes, functionCatalogEntry);
    auto gdsFunc = *func->constPtrCast<GDSFunction>();
    gdsFunc.gds->bind(children, this, graphEntry);
    auto outExprs = gdsFunc.gds->getResultColumns(this);
    auto info = BoundGDSCallInfo(gdsFunc.copy(), graphEntry.copy(), std::move(outExprs));
    return std::make_unique<BoundGDSCall>(std::move(info));
}

} // namespace binder
} // namespace kuzu
