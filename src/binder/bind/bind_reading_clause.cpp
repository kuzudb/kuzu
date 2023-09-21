#include "binder/binder.h"
#include "binder/expression/literal_expression.h"
#include "binder/query/reading_clause/bound_in_query_call.h"
#include "binder/query/reading_clause/bound_load_from.h"
#include "binder/query/reading_clause/bound_match_clause.h"
#include "binder/query/reading_clause/bound_unwind_clause.h"
#include "common/exception/binder.h"
#include "parser/query/reading_clause/in_query_call_clause.h"
#include "parser/query/reading_clause/load_from.h"
#include "parser/query/reading_clause/unwind_clause.h"
#include "processor/operator/persistent/reader/csv_reader.h"

using namespace kuzu::common;
using namespace kuzu::parser;
using namespace kuzu::processor;
using namespace kuzu::catalog;

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
        throw NotImplementedException("bindReadingClause().");
    }
}

std::unique_ptr<BoundReadingClause> Binder::bindMatchClause(const ReadingClause& readingClause) {
    auto& matchClause = reinterpret_cast<const MatchClause&>(readingClause);
    auto [queryGraphCollection, propertyCollection] =
        bindGraphPattern(matchClause.getPatternElements());
    auto boundMatchClause = make_unique<BoundMatchClause>(
        std::move(queryGraphCollection), matchClause.getMatchClauseType());
    std::shared_ptr<Expression> whereExpression;
    if (matchClause.hasWhereClause()) {
        whereExpression = bindWhereExpression(*matchClause.getWhereClause());
    }
    // Rewrite self loop edge
    // e.g. rewrite (a)-[e]->(a) as [a]-[e]->(b) WHERE id(a) = id(b)
    expression_vector selfLoopEdgePredicates;
    auto graphCollection = boundMatchClause->getQueryGraphCollection();
    for (auto i = 0; i < graphCollection->getNumQueryGraphs(); ++i) {
        auto queryGraph = graphCollection->getQueryGraph(i);
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
                src->getInternalIDProperty(), newDst->getInternalIDProperty());
            selfLoopEdgePredicates.push_back(std::move(predicate));
        }
    }
    for (auto& predicate : selfLoopEdgePredicates) {
        whereExpression =
            expressionBinder.combineConjunctiveExpressions(predicate, whereExpression);
    }
    // Rewrite key value pairs in MATCH clause as predicate
    for (auto& [key, val] : propertyCollection->getKeyVals()) {
        auto predicate = expressionBinder.createEqualityComparisonExpression(key, val);
        whereExpression =
            expressionBinder.combineConjunctiveExpressions(predicate, whereExpression);
    }

    boundMatchClause->setWhereExpression(std::move(whereExpression));
    return boundMatchClause;
}

std::unique_ptr<BoundReadingClause> Binder::bindUnwindClause(const ReadingClause& readingClause) {
    auto& unwindClause = reinterpret_cast<const UnwindClause&>(readingClause);
    auto boundExpression = expressionBinder.bindExpression(*unwindClause.getExpression());
    boundExpression =
        ExpressionBinder::implicitCastIfNecessary(boundExpression, LogicalTypeID::VAR_LIST);
    auto aliasExpression = createVariable(
        unwindClause.getAlias(), *VarListType::getChildType(&boundExpression->dataType));
    return make_unique<BoundUnwindClause>(std::move(boundExpression), std::move(aliasExpression));
}

std::unique_ptr<BoundReadingClause> Binder::bindInQueryCall(const ReadingClause& readingClause) {
    auto& call = reinterpret_cast<const InQueryCallClause&>(readingClause);
    auto tableFunctionDefinition =
        catalog.getBuiltInTableFunction()->mathTableFunction(call.getFuncName());
    auto inputValues = std::vector<Value>{};
    for (auto& parameter : call.getParameters()) {
        auto boundExpr = expressionBinder.bindLiteralExpression(*parameter);
        inputValues.push_back(*reinterpret_cast<LiteralExpression*>(boundExpr.get())->getValue());
    }
    auto bindData = tableFunctionDefinition->bindFunc(clientContext,
        function::TableFuncBindInput{std::move(inputValues)}, catalog.getReadOnlyVersion());
    expression_vector outputExpressions;
    for (auto i = 0u; i < bindData->returnColumnNames.size(); i++) {
        outputExpressions.push_back(
            createVariable(bindData->returnColumnNames[i], bindData->returnTypes[i]));
    }
    return std::make_unique<BoundInQueryCall>(
        tableFunctionDefinition->tableFunc, std::move(bindData), std::move(outputExpressions));
}

std::unique_ptr<BoundReadingClause> Binder::bindLoadFrom(
    const parser::ReadingClause& readingClause) {
    auto& loadFrom = reinterpret_cast<const LoadFrom&>(readingClause);
    auto csvReaderConfig = bindParsingOptions(loadFrom.getParsingOptionsRef());
    auto filePaths = bindFilePaths(loadFrom.getFilePaths());
    auto fileType = bindFileType(filePaths);
    auto readerConfig =
        std::make_unique<ReaderConfig>(fileType, std::move(filePaths), std::move(csvReaderConfig));
    if (readerConfig->fileType != FileType::CSV) {
        throw BinderException("Load from non-csv file is not supported.");
    }
    if (readerConfig->getNumFiles() > 1) {
        throw BinderException("Load from multiple files is not supported.");
    }
    auto csvReader = BufferedCSVReader(
        readerConfig->filePaths[0], *readerConfig->csvReaderConfig, 0 /*expectedNumColumns*/);
    csvReader.SniffCSV();
    auto numColumns = csvReader.getNumColumnsDetected();
    expression_vector columns;
    auto stringType = LogicalType(LogicalTypeID::STRING);
    for (auto i = 0; i < numColumns; ++i) {
        auto columnName = "column" + std::to_string(i);
        readerConfig->columnNames.push_back(columnName);
        readerConfig->columnTypes.push_back(stringType.copy());
        columns.push_back(createVariable(columnName, stringType));
    }
    auto info = std::make_unique<BoundFileScanInfo>(
        std::move(readerConfig), std::move(columns), nullptr, TableType::UNKNOWN);
    return std::make_unique<BoundLoadFrom>(std::move(info));
}

} // namespace binder
} // namespace kuzu
