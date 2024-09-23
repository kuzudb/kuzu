#include "binder/binder.h"
#include "binder/copy/bound_copy_from.h"
#include "catalog/catalog.h"
#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "catalog/catalog_entry/rdf_graph_catalog_entry.h"
#include "common/constants.h"
#include "common/copier_config/rdf_reader_config.h"
#include "common/keyword/rdf_keyword.h"
#include "function/built_in_function_utils.h"
#include "function/table/bind_input.h"
#include "parser/copy.h"
#include "processor/operator/persistent/reader/rdf/rdf_scan.h"

using namespace kuzu::binder;
using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::function;
using namespace kuzu::processor;
using namespace kuzu::parser;

namespace kuzu {
namespace binder {

BoundCopyFromInfo Binder::bindCopyRdfResourceInfo(const RdfReaderConfig& config,
    const TableFuncBindData& bindData, const RDFGraphCatalogEntry& rdfEntry) {
    auto transaction = clientContext->getTx();
    auto catalog = clientContext->getCatalog();
    auto functions = catalog->getFunctions(transaction);
    Function* func = config.inMemory ? BuiltInFunctionsUtils::matchFunction(transaction,
                                           RdfResourceInMemScan::name, functions) :
                                       BuiltInFunctionsUtils::matchFunction(transaction,
                                           RdfResourceScan::name, functions);
    auto scanFunc = func->ptrCast<TableFunction>();
    auto iri = expressionBinder.createVariableExpression(LogicalType::STRING(), rdf::IRI);
    auto columns = expression_vector{iri};
    auto scanInfo = BoundTableScanSourceInfo(*scanFunc, bindData.copy(), columns);
    auto scanSource =
        std::make_unique<BoundTableScanSource>(ScanSourceType::FILE, std::move(scanInfo));
    auto rTableID = rdfEntry.getResourceTableID();
    auto rEntry = catalog->getTableCatalogEntry(transaction, rTableID);
    auto rowOffset = expressionBinder.createVariableExpression(LogicalType::INT64(),
        InternalKeyword::ROW_OFFSET);
    return BoundCopyFromInfo(rEntry, std::move(scanSource), rowOffset, std::move(columns),
        {ColumnEvaluateType::REFERENCE}, nullptr /* extraInfo */);
}

BoundCopyFromInfo Binder::bindCopyRdfLiteralInfo(const RdfReaderConfig& config,
    const TableFuncBindData& bindData, const RDFGraphCatalogEntry& rdfEntry) {
    auto transaction = clientContext->getTx();
    auto catalog = clientContext->getCatalog();
    auto functions = catalog->getFunctions(transaction);
    Function* func = config.inMemory ? BuiltInFunctionsUtils::matchFunction(transaction,
                                           RdfLiteralInMemScan::name, functions) :
                                       BuiltInFunctionsUtils::matchFunction(transaction,
                                           RdfLiteralScan::name, functions);
    auto scanFunc = func->ptrCast<TableFunction>();
    auto val = expressionBinder.createVariableExpression(LogicalType::RDF_VARIANT(), rdf::VAL);
    auto lang = expressionBinder.createVariableExpression(LogicalType::STRING(), rdf::LANG);
    auto columns = expression_vector{val, lang};
    auto scanInfo = BoundTableScanSourceInfo(*scanFunc, bindData.copy(), columns);
    auto scanSource =
        std::make_unique<BoundTableScanSource>(ScanSourceType::FILE, std::move(scanInfo));
    auto lTableID = rdfEntry.getLiteralTableID();
    auto lEntry =
        catalog->getTableCatalogEntry(transaction, lTableID)->ptrCast<NodeTableCatalogEntry>();
    auto valID = expressionBinder.bindExpression(*lEntry->getPrimaryKeyDefinition().defaultExpr);
    auto lCopyColumns = expression_vector{valID, val, lang};
    auto rowOffset = expressionBinder.createVariableExpression(LogicalType::INT64(),
        InternalKeyword::ROW_OFFSET);
    return BoundCopyFromInfo(lEntry, std::move(scanSource), rowOffset, lCopyColumns,
        {ColumnEvaluateType::DEFAULT, ColumnEvaluateType::REFERENCE, ColumnEvaluateType::REFERENCE},
        nullptr /* extraInfo */);
}

BoundCopyFromInfo Binder::bindCopyRdfResourceTriplesInfo(const RdfReaderConfig& config,
    const TableFuncBindData& bindData, const RDFGraphCatalogEntry& rdfEntry) {
    auto transaction = clientContext->getTx();
    auto catalog = clientContext->getCatalog();
    auto functions = catalog->getFunctions(transaction);
    Function* func = config.inMemory ? BuiltInFunctionsUtils::matchFunction(transaction,
                                           RdfResourceTripleInMemScan::name, functions) :
                                       BuiltInFunctionsUtils::matchFunction(transaction,
                                           RdfResourceTripleScan::name, functions);
    auto scanFunc = func->ptrCast<TableFunction>();
    auto s = expressionBinder.createVariableExpression(LogicalType::STRING(), rdf::SUBJECT);
    auto p = expressionBinder.createVariableExpression(LogicalType::STRING(), rdf::PREDICATE);
    auto o = expressionBinder.createVariableExpression(LogicalType::STRING(), rdf::OBJECT);
    auto scanColumns = expression_vector{s, p, o};
    auto scanInfo = BoundTableScanSourceInfo(*scanFunc, bindData.copy(), scanColumns);
    auto scanSource =
        std::make_unique<BoundTableScanSource>(ScanSourceType::FILE, std::move(scanInfo));
    auto rTableID = rdfEntry.getResourceTableID();
    auto rrrTableID = rdfEntry.getResourceTripleTableID();
    auto rrrEntry = catalog->getTableCatalogEntry(transaction, rrrTableID);
    auto sOffset = expressionBinder.createVariableExpression(LogicalType::INT64(),
        InternalKeyword::SRC_OFFSET);
    auto pOffset = expressionBinder.createVariableExpression(LogicalType::INT64(), rdf::PID);
    auto oOffset = expressionBinder.createVariableExpression(LogicalType::INT64(),
        InternalKeyword::DST_OFFSET);
    auto sLookUp = IndexLookupInfo(rTableID, sOffset, s);
    auto pLookUp = IndexLookupInfo(rTableID, pOffset, p);
    auto oLookUp = IndexLookupInfo(rTableID, oOffset, o);
    auto lookupInfos = std::vector<IndexLookupInfo>{sLookUp, pLookUp, oLookUp};
    auto internalIDColumnIndices = std::vector<common::idx_t>{0, 1, 2, 3};
    auto extraInfo = std::make_unique<ExtraBoundCopyRelInfo>(internalIDColumnIndices, lookupInfos);
    auto rowOffset = expressionBinder.createVariableExpression(LogicalType::INT64(),
        InternalKeyword::ROW_OFFSET);
    expression_vector rrrCopyColumns{sOffset, oOffset, rowOffset, pOffset};
    std::vector<ColumnEvaluateType> rrrDefaults{ColumnEvaluateType::REFERENCE,
        ColumnEvaluateType::REFERENCE, ColumnEvaluateType::REFERENCE,
        ColumnEvaluateType::REFERENCE};
    return BoundCopyFromInfo(rrrEntry, std::move(scanSource), rowOffset, rrrCopyColumns,
        rrrDefaults, std::move(extraInfo));
}

BoundCopyFromInfo Binder::bindCopyRdfLiteralTriplesInfo(const RdfReaderConfig& config,
    const TableFuncBindData& bindData, const RDFGraphCatalogEntry& rdfEntry) {
    auto transaction = clientContext->getTx();
    auto catalog = clientContext->getCatalog();
    auto functions = catalog->getFunctions(transaction);
    Function* func = config.inMemory ? BuiltInFunctionsUtils::matchFunction(transaction,
                                           RdfLiteralTripleInMemScan::name, functions) :
                                       BuiltInFunctionsUtils::matchFunction(transaction,
                                           RdfLiteralTripleScan::name, functions);
    auto scanFunc = func->ptrCast<TableFunction>();
    auto s = expressionBinder.createVariableExpression(LogicalType::STRING(), rdf::SUBJECT);
    auto p = expressionBinder.createVariableExpression(LogicalType::STRING(), rdf::PREDICATE);
    auto oOffset = expressionBinder.createVariableExpression(LogicalType::INT64(),
        InternalKeyword::DST_OFFSET);
    auto scanColumns = expression_vector{s, p, oOffset};
    auto scanInfo = BoundTableScanSourceInfo(*scanFunc, bindData.copy(), scanColumns);
    auto scanSource =
        std::make_unique<BoundTableScanSource>(ScanSourceType::FILE, std::move(scanInfo));
    auto rTableID = rdfEntry.getResourceTableID();
    auto rrlTableID = rdfEntry.getLiteralTripleTableID();
    auto rrlEntry = catalog->getTableCatalogEntry(transaction, rrlTableID);
    auto sOffset = expressionBinder.createVariableExpression(LogicalType::INT64(),
        InternalKeyword::SRC_OFFSET);
    auto pOffset = expressionBinder.createVariableExpression(LogicalType::INT64(), rdf::PID);
    auto sLookUp = IndexLookupInfo(rTableID, sOffset, s);
    auto pLookUp = IndexLookupInfo(rTableID, pOffset, p);
    auto lookupInfos = std::vector<IndexLookupInfo>{sLookUp, pLookUp};
    auto internalIDColumnIndices = std::vector<common::idx_t>{0, 1, 2, 3};
    auto extraInfo = std::make_unique<ExtraBoundCopyRelInfo>(internalIDColumnIndices, lookupInfos);
    auto rowOffset = expressionBinder.createVariableExpression(LogicalType::INT64(),
        InternalKeyword::ROW_OFFSET);
    expression_vector rrlCopyColumns{sOffset, oOffset, rowOffset, pOffset};
    std::vector<ColumnEvaluateType> rrlDefaults{ColumnEvaluateType::REFERENCE,
        ColumnEvaluateType::REFERENCE, ColumnEvaluateType::REFERENCE,
        ColumnEvaluateType::REFERENCE};
    return BoundCopyFromInfo(rrlEntry, std::move(scanSource), rowOffset, rrlCopyColumns,
        rrlDefaults, std::move(extraInfo));
}

std::unique_ptr<BoundStatement> Binder::bindCopyRdfFrom(const Statement& statement,
    RDFGraphCatalogEntry* rdfGraphEntry) {
    auto& copyStatement = statement.constCast<CopyFrom>();
    // Bind path.
    KU_ASSERT(copyStatement.getSource()->type == ScanSourceType::FILE);
    auto fileSource = copyStatement.getSource()->constPtrCast<FileScanSource>();
    auto filePaths = bindFilePaths(fileSource->filePaths);
    // Bind file type.
    auto fileTypeInfo = bindFileTypeInfo(filePaths);
    // Bind configurations.
    auto config = std::make_unique<ReaderConfig>(fileTypeInfo, std::move(filePaths));
    config->options = bindParsingOptions(copyStatement.getParsingOptionsRef());
    auto catalog = clientContext->getCatalog();
    auto transaction = clientContext->getTx();
    auto functions = catalog->getFunctions(transaction);
    // TODO remove me
    auto bindInput = std::make_unique<ScanTableFuncBindInput>(config->copy());
    // Bind file scan;
    auto rdfConfig = RdfReaderConfig::construct(config->options);
    auto func =
        BuiltInFunctionsUtils::matchFunction(transaction, RdfAllTripleScan::name, functions);
    auto scanFunc = func->ptrCast<TableFunction>();
    auto bindData = scanFunc->bindFunc(clientContext, bindInput.get());
    auto rCopyInfo = bindCopyRdfResourceInfo(rdfConfig, *bindData, *rdfGraphEntry);
    auto lCopyInfo = bindCopyRdfLiteralInfo(rdfConfig, *bindData, *rdfGraphEntry);
    auto rrrCopyInfo = bindCopyRdfResourceTriplesInfo(rdfConfig, *bindData, *rdfGraphEntry);
    auto rrlCopyInfo = bindCopyRdfLiteralTriplesInfo(rdfConfig, *bindData, *rdfGraphEntry);
    auto rdfExtraInfo = std::make_unique<ExtraBoundCopyRdfInfo>(std::move(rCopyInfo),
        std::move(lCopyInfo), std::move(rrrCopyInfo), std::move(rrlCopyInfo));
    std::unique_ptr<BoundBaseScanSource> source;
    if (rdfConfig.inMemory) {
        auto scanInfo = BoundTableScanSourceInfo(*scanFunc, bindData->copy(), expression_vector{});
        source = std::make_unique<BoundTableScanSource>(ScanSourceType::FILE, std::move(scanInfo));
    } else {
        source = std::make_unique<BoundEmptyScanSource>();
    }
    auto rowOffset = expressionBinder.createVariableExpression(LogicalType::INT64(),
        InternalKeyword::ROW_OFFSET);
    auto rdfCopyInfo = BoundCopyFromInfo(rdfGraphEntry, std::move(source), rowOffset, {}, {},
        std::move(rdfExtraInfo));
    return std::make_unique<BoundCopyFrom>(std::move(rdfCopyInfo));
}

} // namespace binder
} // namespace kuzu
