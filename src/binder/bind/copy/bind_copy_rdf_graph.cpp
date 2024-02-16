#include "binder/binder.h"
#include "binder/copy/bound_copy_from.h"
#include "catalog/catalog_entry/rdf_graph_catalog_entry.h"
#include "common/constants.h"
#include "common/copier_config/rdf_reader_config.h"
#include "common/keyword/rdf_keyword.h"
#include "function/table_functions/bind_input.h"
#include "main/client_context.h"

using namespace kuzu::binder;
using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::function;
using namespace kuzu::processor;

namespace kuzu {
namespace binder {

std::unique_ptr<BoundStatement> Binder::bindCopyRdfFrom(const parser::Statement&,
    std::unique_ptr<ReaderConfig> config, RDFGraphCatalogEntry* rdfGraphEntry) {
    auto functions = catalog.getBuiltInFunctions(clientContext->getTx());
    auto offset = expressionBinder.createVariableExpression(
        *LogicalType::INT64(), InternalKeyword::ROW_OFFSET);
    auto r = expressionBinder.createVariableExpression(*LogicalType::STRING(), rdf::IRI);
    auto l = expressionBinder.createVariableExpression(*LogicalType::RDF_VARIANT(), rdf::VAL);
    auto lang = expressionBinder.createVariableExpression(*LogicalType::STRING(), rdf::LANG);
    auto s = expressionBinder.createVariableExpression(*LogicalType::STRING(), rdf::SUBJECT);
    auto p = expressionBinder.createVariableExpression(*LogicalType::STRING(), rdf::PREDICATE);
    auto o = expressionBinder.createVariableExpression(*LogicalType::STRING(), rdf::OBJECT);
    auto sOffset = expressionBinder.createVariableExpression(
        *LogicalType::INT64(), InternalKeyword::SRC_OFFSET);
    auto pOffset = expressionBinder.createVariableExpression(*LogicalType::INT64(), rdf::PID);
    auto oOffset = expressionBinder.createVariableExpression(
        *LogicalType::INT64(), InternalKeyword::DST_OFFSET);
    auto bindInput = std::make_unique<ScanTableFuncBindInput>(config->copy());
    Function* func;
    // Bind file scan;
    auto inMemory = RdfReaderConfig::construct(config->options).inMemory;
    func = functions->matchFunction(READ_RDF_ALL_TRIPLE_FUNC_NAME);
    auto scanFunc = ku_dynamic_cast<Function*, TableFunction*>(func);
    auto bindData =
        scanFunc->bindFunc(clientContext, bindInput.get(), (Catalog*)&catalog, storageManager);
    auto scanInfo = std::make_unique<BoundFileScanInfo>(
        scanFunc, bindData->copy(), expression_vector{}, offset);
    // Bind copy resource.
    func = inMemory ? functions->matchFunction(IN_MEM_READ_RDF_RESOURCE_FUNC_NAME) :
                      functions->matchFunction(READ_RDF_RESOURCE_FUNC_NAME);
    auto rScanFunc = ku_dynamic_cast<Function*, TableFunction*>(func);
    auto rColumns = expression_vector{r};
    auto rScanInfo = std::make_unique<BoundFileScanInfo>(
        rScanFunc, bindData->copy(), std::move(rColumns), offset);
    auto rTableID = rdfGraphEntry->getResourceTableID();
    auto rSchema = catalog.getTableCatalogEntry(clientContext->getTx(), rTableID);
    auto rCopyInfo = BoundCopyFromInfo(rSchema, std::move(rScanInfo), false, nullptr);
    // Bind copy literal.
    func = inMemory ? functions->matchFunction(IN_MEM_READ_RDF_LITERAL_FUNC_NAME) :
                      functions->matchFunction(READ_RDF_LITERAL_FUNC_NAME);
    auto lScanFunc = ku_dynamic_cast<Function*, TableFunction*>(func);
    auto lColumns = expression_vector{l, lang};
    auto lScanInfo = std::make_unique<BoundFileScanInfo>(
        lScanFunc, bindData->copy(), std::move(lColumns), offset);
    auto lTableID = rdfGraphEntry->getLiteralTableID();
    auto lSchema = catalog.getTableCatalogEntry(clientContext->getTx(), lTableID);
    auto lCopyInfo = BoundCopyFromInfo(lSchema, std::move(lScanInfo), true, nullptr);
    // Bind copy resource triples
    func = inMemory ? functions->matchFunction(IN_MEM_READ_RDF_RESOURCE_TRIPLE_FUNC_NAME) :
                      functions->matchFunction(READ_RDF_RESOURCE_TRIPLE_FUNC_NAME);
    auto rrrScanFunc = ku_dynamic_cast<Function*, TableFunction*>(func);
    auto rrrColumns = expression_vector{s, p, o};
    auto rrrScanInfo =
        std::make_unique<BoundFileScanInfo>(rrrScanFunc, bindData->copy(), rrrColumns, offset);
    auto rrrTableID = rdfGraphEntry->getResourceTripleTableID();
    auto rrrSchema = catalog.getTableCatalogEntry(clientContext->getTx(), rrrTableID);
    auto rrrExtraInfo = std::make_unique<ExtraBoundCopyRelInfo>();
    auto sLookUp = IndexLookupInfo(rTableID, sOffset, s, s->getDataType());
    auto pLookUp = IndexLookupInfo(rTableID, pOffset, p, p->getDataType());
    auto oLookUp = IndexLookupInfo(rTableID, oOffset, o, o->getDataType());
    rrrExtraInfo->infos.push_back(sLookUp.copy());
    rrrExtraInfo->infos.push_back(pLookUp.copy());
    rrrExtraInfo->infos.push_back(oLookUp.copy());
    rrrExtraInfo->fromOffset = sOffset;
    rrrExtraInfo->toOffset = oOffset;
    auto rrrCopyInfo =
        BoundCopyFromInfo(rrrSchema, std::move(rrrScanInfo), false, std::move(rrrExtraInfo));
    // Bind copy literal triples
    func = inMemory ? functions->matchFunction(IN_MEM_READ_RDF_LITERAL_TRIPLE_FUNC_NAME) :
                      functions->matchFunction(READ_RDF_LITERAL_TRIPLE_FUNC_NAME);
    auto rrlScanFunc = ku_dynamic_cast<Function*, TableFunction*>(func);
    auto rrlColumns = expression_vector{s, p, oOffset};
    auto rrlScanInfo =
        std::make_unique<BoundFileScanInfo>(rrlScanFunc, bindData->copy(), rrlColumns, offset);
    auto rrlTableID = rdfGraphEntry->getLiteralTripleTableID();
    auto rrlSchema = catalog.getTableCatalogEntry(clientContext->getTx(), rrlTableID);
    auto rrlExtraInfo = std::make_unique<ExtraBoundCopyRelInfo>();
    rrlExtraInfo->infos.push_back(sLookUp.copy());
    rrlExtraInfo->infos.push_back(pLookUp.copy());
    rrlExtraInfo->propertyColumns.push_back(oOffset);
    rrlExtraInfo->fromOffset = sOffset;
    rrlExtraInfo->toOffset = oOffset;
    auto rrLCopyInfo =
        BoundCopyFromInfo(rrlSchema, std::move(rrlScanInfo), false, std::move(rrlExtraInfo));
    // Bind copy rdf
    auto rdfExtraInfo = std::make_unique<ExtraBoundCopyRdfInfo>(
        std::move(rCopyInfo), std::move(lCopyInfo), std::move(rrrCopyInfo), std::move(rrLCopyInfo));
    auto rdfCopyInfo = BoundCopyFromInfo(
        rdfGraphEntry, inMemory ? std::move(scanInfo) : nullptr, false, std::move(rdfExtraInfo));
    return std::make_unique<BoundCopyFrom>(std::move(rdfCopyInfo));
}

} // namespace binder
} // namespace kuzu
