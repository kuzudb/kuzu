#include "binder/binder.h"
#include "binder/expression/variable_expression.h"
#include "binder/query/reading_clause/bound_in_query_call.h"
#include "binder/query/reading_clause/bound_load_from.h"
#include "binder/query/reading_clause/bound_match_clause.h"
#include "binder/query/reading_clause/bound_unwind_clause.h"
#include "common/exception/binder.h"
#include "common/string_format.h"
#include "common/string_utils.h"
#include "function/table_functions/bind_input.h"
#include "parser/expression/parsed_function_expression.h"
#include "parser/expression/parsed_literal_expression.h"
#include "parser/query/reading_clause/in_query_call_clause.h"
#include "parser/query/reading_clause/load_from.h"
#include "parser/query/reading_clause/match_clause.h"
#include "parser/query/reading_clause/unwind_clause.h"
#include "processor/operator/persistent/reader/csv/serial_csv_reader.h"
#include "processor/operator/persistent/reader/npy/npy_reader.h"
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
        KU_UNREACHABLE;
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
        whereExpression = expressionBinder.combineBooleanExpressions(
            ExpressionType::AND, predicate, whereExpression);
    }
    // Rewrite key value pairs in MATCH clause as predicate
    for (auto& [key, val] : propertyCollection->getKeyVals()) {
        auto predicate = expressionBinder.createEqualityComparisonExpression(key, val);
        whereExpression = expressionBinder.combineBooleanExpressions(
            ExpressionType::AND, predicate, whereExpression);
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
    auto funcExpr = reinterpret_cast<ParsedFunctionExpression*>(call.getFunctionExpression());
    std::vector<std::unique_ptr<Value>> inputValues;
    std::vector<LogicalType*> inputTypes;
    for (auto i = 0u; i < funcExpr->getNumChildren(); i++) {
        auto parameter = funcExpr->getChild(i);
        if (parameter->getExpressionType() != ExpressionType::LITERAL) {
            throw BinderException{"Parameters in table function must be a literal expression."};
        }
        auto expressionValue = reinterpret_cast<ParsedLiteralExpression*>(parameter)->getValue();
        inputTypes.push_back(expressionValue->getDataType());
        inputValues.push_back(expressionValue->copy());
    }
    auto funcNameToMatch = funcExpr->getFunctionName();
    StringUtils::toUpper(funcNameToMatch);
    auto tableFunction = reinterpret_cast<function::TableFunction*>(
        catalog.getBuiltInFunctions()->matchScalarFunction(std::move(funcNameToMatch), inputTypes));
    std::unique_ptr<function::TableFuncBindInput> tableFuncBindInput;
    tableFuncBindInput = std::make_unique<function::TableFuncBindInput>(std::move(inputValues));
    auto bindData = tableFunction->bindFunc(
        clientContext, tableFuncBindInput.get(), catalog.getReadOnlyVersion());
    expression_vector outputExpressions;
    for (auto i = 0u; i < bindData->returnColumnNames.size(); i++) {
        outputExpressions.push_back(
            createVariable(bindData->returnColumnNames[i], *bindData->returnTypes[i]));
    }
    return std::make_unique<BoundInQueryCall>(std::move(tableFunction), std::move(bindData),
        std::move(outputExpressions),
        std::make_shared<VariableExpression>(LogicalType{LogicalTypeID::INT64},
            getUniqueExpressionName(CopyConstants::ROW_IDX_COLUMN_NAME),
            CopyConstants::ROW_IDX_COLUMN_NAME));
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
    // Bind columns from input.
    std::vector<std::string> expectedColumnNames;
    std::vector<std::unique_ptr<common::LogicalType>> expectedColumnTypes;
    for (auto& [name, type] : loadFrom.getColumnNameDataTypesRef()) {
        expectedColumnNames.push_back(name);
        expectedColumnTypes.push_back(bindDataType(type));
    }
    // Detect columns from file.
    std::vector<std::string> detectedColumnNames;
    std::vector<std::unique_ptr<common::LogicalType>> detectedColumnTypes;
    sniffFiles(*readerConfig, detectedColumnNames, detectedColumnTypes);
    // Validate and resolve columns to use.
    expression_vector columns;
    if (expectedColumnTypes.empty()) { // Input is empty. Use detected columns.
        columns = createColumnExpressions(*readerConfig, detectedColumnNames, detectedColumnTypes);
    } else {
        validateNumColumns(expectedColumnTypes.size(), detectedColumnTypes.size());
        if (fileType == common::FileType::PARQUET) {
            validateColumnTypes(expectedColumnNames, expectedColumnTypes, detectedColumnTypes);
        }
        columns = createColumnExpressions(*readerConfig, expectedColumnNames, expectedColumnTypes);
    }
    auto inputType = std::make_unique<LogicalType>(LogicalTypeID::STRING);
    std::vector<LogicalType*> inputTypes;
    inputTypes.push_back(inputType.get());
    auto scanFunction = getScanFunction(
        readerConfig->fileType, std::move(inputTypes), readerConfig->csvReaderConfig->parallel);
    std::vector<std::unique_ptr<Value>> inputValues;
    inputValues.push_back(std::make_unique<Value>(Value::createValue(readerConfig->filePaths[0])));
    std::unique_ptr<function::TableFuncBindInput> tableFuncBindInput =
        std::make_unique<function::ScanTableFuncBindInput>(*readerConfig, memoryManager);
    auto bindData = scanFunction->bindFunc(
        clientContext, tableFuncBindInput.get(), catalog.getReadOnlyVersion());
    auto info =
        std::make_unique<BoundFileScanInfo>(scanFunction, std::move(bindData), std::move(columns),
            std::make_shared<VariableExpression>(LogicalType{LogicalTypeID::INT64},
                getUniqueExpressionName(CopyConstants::ROW_IDX_COLUMN_NAME),
                CopyConstants::ROW_IDX_COLUMN_NAME),
            TableType::UNKNOWN);
    auto boundLoadFrom = std::make_unique<BoundLoadFrom>(std::move(info));
    if (loadFrom.hasWherePredicate()) {
        auto wherePredicate = expressionBinder.bindExpression(*loadFrom.getWherePredicate());
        boundLoadFrom->setWherePredicate(std::move(wherePredicate));
    }
    return boundLoadFrom;
}

expression_vector Binder::createColumnExpressions(common::ReaderConfig& readerConfig,
    const std::vector<std::string>& columnNames,
    const std::vector<std::unique_ptr<common::LogicalType>>& columnTypes) {
    expression_vector columns;
    for (auto i = 0u; i < columnTypes.size(); ++i) {
        auto columnName = columnNames[i];
        auto columnType = columnTypes[i].get();
        readerConfig.columnNames.push_back(columnName);
        readerConfig.columnTypes.push_back(columnType->copy());
        columns.push_back(createVariable(columnName, *columnType));
    }
    return columns;
}

void Binder::validateColumnTypes(const std::vector<std::string>& columnNames,
    const std::vector<std::unique_ptr<LogicalType>>& expectedColumnTypes,
    const std::vector<std::unique_ptr<LogicalType>>& detectedColumnTypes) {
    KU_ASSERT(expectedColumnTypes.size() == detectedColumnTypes.size());
    for (auto i = 0; i < expectedColumnTypes.size(); ++i) {
        if (*expectedColumnTypes[i] != *detectedColumnTypes[i]) {
            throw BinderException(
                stringFormat("Column `{}` type mismatch. Expected {} but got {}.", columnNames[i],
                    expectedColumnTypes[i]->toString(), detectedColumnTypes[i]->toString()));
        }
    }
}

void Binder::validateNumColumns(uint32_t expectedNumber, uint32_t detectedNumber) {
    if (detectedNumber == 0) {
        return; // Empty CSV. Continue processing.
    }
    if (expectedNumber != detectedNumber) {
        throw BinderException(stringFormat(
            "Number of columns mismatch. Expected {} but got {}.", expectedNumber, detectedNumber));
    }
}

void Binder::sniffFiles(const common::ReaderConfig& readerConfig,
    std::vector<std::string>& columnNames,
    std::vector<std::unique_ptr<common::LogicalType>>& columnTypes) {
    KU_ASSERT(readerConfig.getNumFiles() > 0);
    sniffFile(readerConfig, 0, columnNames, columnTypes);
    for (auto i = 1; i < readerConfig.getNumFiles(); ++i) {
        std::vector<std::string> tmpColumnNames;
        std::vector<std::unique_ptr<LogicalType>> tmpColumnTypes;
        sniffFile(readerConfig, i, tmpColumnNames, tmpColumnTypes);
        switch (readerConfig.fileType) {
        case FileType::CSV: {
            validateNumColumns(columnTypes.size(), tmpColumnTypes.size());
        }
        case FileType::PARQUET: {
            validateNumColumns(columnTypes.size(), tmpColumnTypes.size());
            validateColumnTypes(columnNames, columnTypes, tmpColumnTypes);
        } break;
        case FileType::NPY: {
            validateNumColumns(1, tmpColumnTypes.size());
            columnNames.push_back(tmpColumnNames[0]);
            columnTypes.push_back(tmpColumnTypes[0]->copy());
        } break;
        case FileType::TURTLE:
            break;
        default:
            KU_UNREACHABLE;
        }
    }
}

void Binder::sniffFile(const common::ReaderConfig& readerConfig, uint32_t fileIdx,
    std::vector<std::string>& columnNames, std::vector<std::unique_ptr<LogicalType>>& columnTypes) {
    switch (readerConfig.fileType) {
    case FileType::CSV: {
        auto csvReader = SerialCSVReader(readerConfig.filePaths[fileIdx], readerConfig);
        auto sniffedColumns = csvReader.sniffCSV();
        for (auto& [name, type] : sniffedColumns) {
            columnNames.push_back(name);
            columnTypes.push_back(type.copy());
        }
    } break;
    case FileType::PARQUET: {
        auto reader = ParquetReader(readerConfig.filePaths[fileIdx], memoryManager);
        auto state = std::make_unique<processor::ParquetReaderScanState>();
        reader.initializeScan(*state, std::vector<uint64_t>{});
        for (auto i = 0u; i < reader.getNumColumns(); ++i) {
            columnNames.push_back(reader.getColumnName(i));
            columnTypes.push_back(reader.getColumnType(i)->copy());
        }
    } break;
    case FileType::NPY: {
        auto reader = NpyReader(readerConfig.filePaths[0]);
        auto columnName = std::string("column" + std::to_string(fileIdx));
        auto columnType = bindFixedListType(reader.getShape(), reader.getType());
        columnNames.push_back(columnName);
        columnTypes.push_back(columnType->copy());
    } break;
    default:
        throw BinderException(stringFormat(
            "Cannot sniff header of file type {}", FileTypeUtils::toString(readerConfig.fileType)));
    }
}

} // namespace binder
} // namespace kuzu
