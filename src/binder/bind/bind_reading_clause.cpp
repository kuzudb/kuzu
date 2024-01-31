#include "binder/binder.h"
#include "binder/expression/literal_expression.h"
#include "binder/query/reading_clause/bound_in_query_call.h"
#include "binder/query/reading_clause/bound_load_from.h"
#include "binder/query/reading_clause/bound_match_clause.h"
#include "binder/query/reading_clause/bound_unwind_clause.h"
#include "common/exception/binder.h"
#include "common/string_format.h"
#include "function/table_functions/bind_input.h"
#include "main/client_context.h"
#include "parser/expression/parsed_function_expression.h"
#include "parser/query/reading_clause/in_query_call_clause.h"
#include "parser/query/reading_clause/load_from.h"
#include "parser/query/reading_clause/match_clause.h"
#include "parser/query/reading_clause/unwind_clause.h"

using namespace kuzu::common;
using namespace kuzu::parser;
using namespace kuzu::catalog;
using namespace kuzu::function;

namespace kuzu {
namespace binder {

std::unique_ptr<BoundReadingClause> Binder::bindReadingClause(const ReadingClause& readingClause) {
    switch (readingClause.getClauseType()) {
    case ClauseType::MATCH: {
        return bindMatchClause(readingClause);
    }
    case ClauseType::UNWIND: {
        return bindUnwindClause(readingClause);
    }
    case ClauseType::IN_QUERY_CALL: {
        return bindInQueryCall(readingClause);
    }
    case ClauseType::LOAD_FROM: {
        return bindLoadFrom(readingClause);
    }
    default:
        KU_UNREACHABLE;
    }
}

std::unique_ptr<BoundReadingClause> Binder::bindMatchClause(const ReadingClause& readingClause) {
    auto& matchClause = ku_dynamic_cast<const ReadingClause&, const MatchClause&>(readingClause);
    auto boundGraphPattern = bindGraphPattern(matchClause.getPatternElementsRef());
    if (matchClause.hasWherePredicate()) {
        boundGraphPattern.where = bindWhereExpression(*matchClause.getWherePredicate());
    }
    rewriteMatchPattern(boundGraphPattern);
    auto boundMatch = std::make_unique<BoundMatchClause>(
        std::move(boundGraphPattern.queryGraphCollection), matchClause.getMatchClauseType());
    boundMatch->setPredicate(boundGraphPattern.where);
    return boundMatch;
}

void Binder::rewriteMatchPattern(BoundGraphPattern& boundGraphPattern) {
    // Rewrite self loop edge
    // e.g. rewrite (a)-[e]->(a) as [a]-[e]->(b) WHERE id(a) = id(b)
    expression_vector selfLoopEdgePredicates;
    auto& graphCollection = boundGraphPattern.queryGraphCollection;
    for (auto i = 0u; i < graphCollection.getNumQueryGraphs(); ++i) {
        auto queryGraph = graphCollection.getQueryGraphUnsafe(i);
        for (auto& queryRel : queryGraph->getQueryRels()) {
            if (!queryRel->isSelfLoop()) {
                continue;
            }
            auto src = queryRel->getSrcNode();
            auto dst = queryRel->getDstNode();
            auto newDst = createQueryNode(dst->getVariableName(), dst->getTableIDs());
            queryGraph->addQueryNode(newDst);
            queryRel->setDstNode(newDst);
            auto predicate = expressionBinder.createEqualityComparisonExpression(
                src->getInternalID(), newDst->getInternalID());
            selfLoopEdgePredicates.push_back(std::move(predicate));
        }
    }
    auto where = boundGraphPattern.where;
    for (auto& predicate : selfLoopEdgePredicates) {
        where = expressionBinder.combineBooleanExpressions(ExpressionType::AND, predicate, where);
    }
    // Rewrite key value pairs in MATCH clause as predicate
    for (auto i = 0u; i < graphCollection.getNumQueryGraphs(); ++i) {
        auto queryGraph = graphCollection.getQueryGraphUnsafe(i);
        for (auto& pattern : queryGraph->getAllPatterns()) {
            for (auto& [propertyName, rhs] : pattern->getPropertyDataExprRef()) {
                auto propertyExpr =
                    expressionBinder.bindNodeOrRelPropertyExpression(*pattern, propertyName);
                auto predicate =
                    expressionBinder.createEqualityComparisonExpression(propertyExpr, rhs);
                where = expressionBinder.combineBooleanExpressions(
                    ExpressionType::AND, predicate, where);
            }
        }
    }
    boundGraphPattern.where = std::move(where);
}

std::unique_ptr<BoundReadingClause> Binder::bindUnwindClause(const ReadingClause& readingClause) {
    auto& unwindClause = ku_dynamic_cast<const ReadingClause&, const UnwindClause&>(readingClause);
    auto boundExpression = expressionBinder.bindExpression(*unwindClause.getExpression());
    boundExpression =
        ExpressionBinder::implicitCastIfNecessary(boundExpression, LogicalTypeID::VAR_LIST);
    auto aliasExpression = createVariable(
        unwindClause.getAlias(), *VarListType::getChildType(&boundExpression->dataType));
    return make_unique<BoundUnwindClause>(std::move(boundExpression), std::move(aliasExpression));
}

std::unique_ptr<BoundReadingClause> Binder::bindInQueryCall(const ReadingClause& readingClause) {
    auto& call = ku_dynamic_cast<const ReadingClause&, const InQueryCallClause&>(readingClause);
    auto expr = call.getFunctionExpression();
    auto functionExpr =
        ku_dynamic_cast<const ParsedExpression*, const ParsedFunctionExpression*>(expr);
    std::vector<Value> inputValues;
    for (auto i = 0u; i < functionExpr->getNumChildren(); i++) {
        auto parameter = expressionBinder.bindExpression(*functionExpr->getChild(i));
        if (parameter->expressionType != ExpressionType::LITERAL) {
            throw BinderException{
                stringFormat("Cannot evaluate {} as a literal.", parameter->toString())};
        }
        auto literalExpr =
            ku_dynamic_cast<const Expression*, const LiteralExpression*>(parameter.get());
        inputValues.push_back(*literalExpr->getValue());
    }
    std::vector<LogicalType> inputTypes;
    for (auto& val : inputValues) {
        inputTypes.push_back(*val.getDataType());
    }
    auto func = catalog.getBuiltInFunctions(clientContext->getTx())
                    ->matchFunction(functionExpr->getFunctionName(), inputTypes);
    auto tableFunc = ku_dynamic_cast<function::Function*, function::TableFunction*>(func);
    auto bindInput = std::make_unique<function::TableFuncBindInput>();
    bindInput->inputs = std::move(inputValues);
    auto bindData =
        tableFunc->bindFunc(clientContext, bindInput.get(), (Catalog*)&catalog, storageManager);
    expression_vector columns;
    for (auto i = 0u; i < bindData->columnTypes.size(); i++) {
        columns.push_back(createVariable(bindData->columnNames[i], bindData->columnTypes[i]));
    }
    auto offset = expressionBinder.createVariableExpression(
        *LogicalType::INT64(), std::string(InternalKeyword::ROW_OFFSET));
    auto boundInQueryCall = std::make_unique<BoundInQueryCall>(
        tableFunc, std::move(bindData), std::move(columns), offset);
    if (call.hasWherePredicate()) {
        auto wherePredicate = expressionBinder.bindExpression(*call.getWherePredicate());
        boundInQueryCall->setPredicate(std::move(wherePredicate));
    }
    return boundInQueryCall;
}

std::unique_ptr<BoundReadingClause> Binder::bindLoadFrom(const ReadingClause& readingClause) {
    auto& loadFrom = ku_dynamic_cast<const ReadingClause&, const LoadFrom&>(readingClause);
    function::TableFunction* scanFunction;
    std::unique_ptr<TableFuncBindInput> bindInput;
    if (loadFrom.hasObjectName()) {
        auto objectName = loadFrom.getObjectname();
        auto objectExpr = expressionBinder.bindVariableExpression(objectName);
        auto literalExpr =
            ku_dynamic_cast<const Expression*, const LiteralExpression*>(objectExpr.get());
        auto func = catalog.getBuiltInFunctions(clientContext->getTx())
                        ->matchFunction(READ_PANDAS_FUNC_NAME,
                            std::vector<LogicalType>{objectExpr->getDataType()});
        scanFunction = ku_dynamic_cast<Function*, TableFunction*>(func);
        bindInput = std::make_unique<function::TableFuncBindInput>();
        bindInput->inputs.push_back(*literalExpr->getValue());
    } else {
        auto filePaths = bindFilePaths(loadFrom.getFilePaths());
        auto fileType = bindFileType(filePaths);
        auto readerConfig = std::make_unique<ReaderConfig>(fileType, std::move(filePaths));
        readerConfig->options = bindParsingOptions(loadFrom.getParsingOptionsRef());
        if (readerConfig->getNumFiles() > 1) {
            throw BinderException("Load from multiple files is not supported.");
        }
        switch (fileType) {
        case common::FileType::CSV:
        case common::FileType::PARQUET:
        case common::FileType::NPY:
            break;
        default:
            throw BinderException(
                stringFormat("Cannot load from file type {}.", FileTypeUtils::toString(fileType)));
        }
        // Bind columns from input.
        std::vector<std::string> expectedColumnNames;
        std::vector<common::LogicalType> expectedColumnTypes;
        for (auto& [name, type] : loadFrom.getColumnNameDataTypesRef()) {
            expectedColumnNames.push_back(name);
            expectedColumnTypes.push_back(*bindDataType(type));
        }
        scanFunction = getScanFunction(readerConfig->fileType, *readerConfig);
        auto bindInput_ = std::make_unique<ScanTableFuncBindInput>(readerConfig->copy());
        bindInput_->expectedColumnNames = std::move(expectedColumnNames);
        bindInput_->expectedColumnTypes = std::move(expectedColumnTypes);
        bindInput_->context = clientContext;
        bindInput = std::move(bindInput_);
    }
    auto bindData =
        scanFunction->bindFunc(clientContext, bindInput.get(), (Catalog*)&catalog, storageManager);
    expression_vector columns;
    for (auto i = 0u; i < bindData->columnTypes.size(); i++) {
        columns.push_back(createVariable(bindData->columnNames[i], bindData->columnTypes[i]));
    }
    auto offset = expressionBinder.createVariableExpression(
        LogicalType(LogicalTypeID::INT64), std::string(InternalKeyword::ROW_OFFSET));
    auto info =
        BoundFileScanInfo(scanFunction, std::move(bindData), std::move(columns), std::move(offset));
    auto boundLoadFrom = std::make_unique<BoundLoadFrom>(std::move(info));
    if (loadFrom.hasWherePredicate()) {
        auto wherePredicate = expressionBinder.bindExpression(*loadFrom.getWherePredicate());
        boundLoadFrom->setPredicate(std::move(wherePredicate));
    }
    return boundLoadFrom;
}

} // namespace binder
} // namespace kuzu
