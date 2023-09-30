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
#include "processor/operator/persistent/reader/csv/serial_csv_reader.h"
#include "processor/operator/persistent/reader/npy_reader.h"
#include "processor/operator/persistent/reader/parquet/parquet_reader.h"

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
    if (matchClause.hasWherePredicate()) {
        whereExpression = bindWhereExpression(*matchClause.getWherePredicate());
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
                src->getInternalID(), newDst->getInternalID());
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

    boundMatchClause->setWherePredicate(std::move(whereExpression));
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

static std::unique_ptr<LogicalType> bindFixedListType(
    const std::vector<size_t>& shape, LogicalTypeID typeID) {
    if (shape.size() == 1) {
        return std::make_unique<LogicalType>(typeID);
    }
    auto childShape = std::vector<size_t>{shape.begin() + 1, shape.end()};
    auto childType = bindFixedListType(childShape, typeID);
    auto extraInfo = std::make_unique<FixedListTypeInfo>(std::move(childType), (uint32_t)shape[0]);
    return std::make_unique<LogicalType>(LogicalTypeID::FIXED_LIST, std::move(extraInfo));
}

std::unique_ptr<BoundReadingClause> Binder::bindLoadFrom(
    const parser::ReadingClause& readingClause) {
    auto& loadFrom = reinterpret_cast<const LoadFrom&>(readingClause);
    auto csvReaderConfig = bindParsingOptions(loadFrom.getParsingOptionsRef());
    auto filePaths = bindFilePaths(loadFrom.getFilePaths());
    auto fileType = bindFileType(filePaths);
    auto readerConfig =
        std::make_unique<ReaderConfig>(fileType, std::move(filePaths), std::move(csvReaderConfig));
    if (readerConfig->getNumFiles() > 1) {
        throw BinderException("Load from multiple files is not supported.");
    }
    std::vector<std::string> inputColumnNames;
    std::vector<std::unique_ptr<common::LogicalType>> inputColumnTypes;
    for (auto& [name, type] : loadFrom.getColumnNameDataTypesRef()) {
        inputColumnNames.push_back(name);
        inputColumnTypes.push_back(bindDataType(type));
    }
    std::vector<std::string> detectedColumnNames;
    std::vector<std::unique_ptr<common::LogicalType>> detectedColumnTypes;
    switch (fileType) {
    case FileType::CSV: {
        auto csvReader = SerialCSVReader(readerConfig->filePaths[0], *readerConfig);
        auto sniffedColumns = csvReader.sniffCSV();
        for (auto& [name, type] : sniffedColumns) {
            detectedColumnNames.push_back(name);
            detectedColumnTypes.push_back(type.copy());
        }
    } break;
    case FileType::PARQUET: {
        auto reader = ParquetReader(readerConfig->filePaths[0], memoryManager);
        auto state = std::make_unique<processor::ParquetReaderScanState>();
        reader.initializeScan(*state, std::vector<uint64_t>{});
        for (auto i = 0u; i < reader.getNumColumns(); ++i) {
            detectedColumnNames.push_back(reader.getColumnName(i));
            detectedColumnTypes.push_back(reader.getColumnType(i)->copy());
        }
    } break;
    case FileType::NPY: {
        auto reader = NpyReader(readerConfig->filePaths[0]);
        auto columnName = std::string("column0");
        auto columnType = bindFixedListType(reader.getShape(), reader.getType());
        detectedColumnNames.push_back(columnName);
        detectedColumnTypes.push_back(columnType->copy());
    } break;
    default:
        throw BinderException(StringUtils::string_format(
            "Load from {} file is not supported.", FileTypeUtils::toString(fileType)));
    }
    expression_vector columns;
    if (inputColumnTypes.empty()) {
        for (auto i = 0u; i < detectedColumnTypes.size(); ++i) {
            auto columnName = detectedColumnNames[i];
            auto columnType = detectedColumnTypes[i].get();
            readerConfig->columnNames.push_back(columnName);
            readerConfig->columnTypes.push_back(columnType->copy());
            columns.push_back(createVariable(columnName, *columnType));
        }
    } else {
        if (inputColumnTypes.size() != detectedColumnTypes.size()) {
            throw BinderException(
                StringUtils::string_format("Number of columns mismatch. Detect {} but expect {}.",
                    detectedColumnTypes.size(), inputColumnTypes.size()));
        }
        if (fileType == common::FileType::PARQUET) {
            for (auto i = 0u; i < inputColumnTypes.size(); ++i) {
                auto inputType = inputColumnTypes[i].get();
                auto detectType = detectedColumnTypes[i].get();
                if (*inputType != *detectType) {
                    throw BinderException(StringUtils::string_format(
                        "Column {} data type mismatch. Detect {} but expect {}.",
                        inputColumnNames[i], LogicalTypeUtils::dataTypeToString(*detectType),
                        LogicalTypeUtils::dataTypeToString(*inputType)));
                }
            }
        }
        for (auto i = 0u; i < inputColumnTypes.size(); ++i) {
            auto columnName = inputColumnNames[i];
            auto columnType = inputColumnTypes[i].get();
            readerConfig->columnNames.push_back(columnName);
            readerConfig->columnTypes.push_back(columnType->copy());
            columns.push_back(createVariable(columnName, *columnType));
        }
    }
    auto info = std::make_unique<BoundFileScanInfo>(
        std::move(readerConfig), std::move(columns), nullptr, TableType::UNKNOWN);
    auto boundLoadFrom = std::make_unique<BoundLoadFrom>(std::move(info));
    if (loadFrom.hasWherePredicate()) {
        auto wherePredicate = expressionBinder.bindExpression(*loadFrom.getWherePredicate());
        boundLoadFrom->setWherePredicate(std::move(wherePredicate));
    }
    return boundLoadFrom;
}

} // namespace binder
} // namespace kuzu
