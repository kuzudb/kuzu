#include "binder/binder.h"
#include "binder/copy/bound_copy_from.h"
#include "catalog/catalog.h"
#include "catalog/catalog_entry/rdf_graph_catalog_entry.h"
#include "common/constants.h"
#include "common/copier_config/rdf_reader_config.h"
#include "common/keyword/rdf_keyword.h"
#include "function/built_in_function_utils.h"
#include "function/table/bind_input.h"
#include "main/client_context.h"
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

std::unique_ptr<BoundStatement> Binder::bindCopyRdfFrom(const parser::Statement& statement,
    RDFGraphCatalogEntry* rdfGraphEntry) {
    auto& copyStatement = ku_dynamic_cast<const Statement&, const CopyFrom&>(statement);
    // Bind path.
    KU_ASSERT(copyStatement.getSource()->type == ScanSourceType::FILE);
    auto fileSource = ku_dynamic_cast<BaseScanSource*, FileScanSource*>(copyStatement.getSource());
    auto filePaths = bindFilePaths(fileSource->filePaths);
    // Bind file type.
    auto fileType = bindFileType(filePaths);
    auto config = std::make_unique<ReaderConfig>(fileType, std::move(filePaths));
    config->options = bindParsingOptions(copyStatement.getParsingOptionsRef());
    auto catalog = clientContext->getCatalog();
    auto functions = catalog->getFunctions(clientContext->getTx());
    auto offset = expressionBinder.createVariableExpression(*LogicalType::INT64(),
        InternalKeyword::ROW_OFFSET);
    auto r = expressionBinder.createVariableExpression(*LogicalType::STRING(), rdf::IRI);
    auto l = expressionBinder.createVariableExpression(*LogicalType::RDF_VARIANT(), rdf::VAL);
    auto lang = expressionBinder.createVariableExpression(*LogicalType::STRING(), rdf::LANG);
    auto s = expressionBinder.createVariableExpression(*LogicalType::STRING(), rdf::SUBJECT);
    auto p = expressionBinder.createVariableExpression(*LogicalType::STRING(), rdf::PREDICATE);
    auto o = expressionBinder.createVariableExpression(*LogicalType::STRING(), rdf::OBJECT);
    auto sOffset = expressionBinder.createVariableExpression(*LogicalType::INT64(),
        InternalKeyword::SRC_OFFSET);
    auto pOffset = expressionBinder.createVariableExpression(*LogicalType::INT64(), rdf::PID);
    auto oOffset = expressionBinder.createVariableExpression(*LogicalType::INT64(),
        InternalKeyword::DST_OFFSET);
    auto bindInput = std::make_unique<ScanTableFuncBindInput>(config->copy());
    Function* func;
    // Bind file scan;
    auto inMemory = RdfReaderConfig::construct(config->options).inMemory;
    func = BuiltInFunctionsUtils::matchFunction(clientContext->getTx(), RdfAllTripleScan::name,
        functions);
    auto scanFunc = ku_dynamic_cast<Function*, TableFunction*>(func);
    auto bindData = scanFunc->bindFunc(clientContext, bindInput.get());
    // Bind copy resource.
    func = inMemory ? BuiltInFunctionsUtils::matchFunction(clientContext->getTx(),
                          RdfResourceInMemScan::name, functions) :
                      BuiltInFunctionsUtils::matchFunction(clientContext->getTx(),
                          RdfResourceScan::name, functions);
    auto rScanFunc = ku_dynamic_cast<Function*, TableFunction*>(func);
    auto rColumns = expression_vector{r};
    auto rFileScanInfo = BoundFileScanInfo(*rScanFunc, bindData->copy(), std::move(rColumns));
    auto rSource = std::make_unique<BoundFileScanSource>(std::move(rFileScanInfo));
    auto rTableID = rdfGraphEntry->getResourceTableID();
    auto rEntry = catalog->getTableCatalogEntry(clientContext->getTx(), rTableID);
    auto rCopyInfo = BoundCopyFromInfo(rEntry, std::move(rSource), offset, nullptr /* extraInfo */);
    // Bind copy literal.
    func = inMemory ? BuiltInFunctionsUtils::matchFunction(clientContext->getTx(),
                          RdfLiteralInMemScan::name, functions) :
                      BuiltInFunctionsUtils::matchFunction(clientContext->getTx(),
                          RdfLiteralScan::name, functions);
    auto lScanFunc = ku_dynamic_cast<Function*, TableFunction*>(func);
    auto lColumns = expression_vector{l, lang};
    auto lFileScanInfo = BoundFileScanInfo(*lScanFunc, bindData->copy(), std::move(lColumns));
    auto lSource = std::make_unique<BoundFileScanSource>(std::move(lFileScanInfo));
    auto lTableID = rdfGraphEntry->getLiteralTableID();
    auto lEntry = catalog->getTableCatalogEntry(clientContext->getTx(), lTableID);
    auto lCopyInfo = BoundCopyFromInfo(lEntry, std::move(lSource), offset, nullptr /* extraInfo */);
    // Bind copy resource triples
    func = inMemory ? BuiltInFunctionsUtils::matchFunction(clientContext->getTx(),
                          RdfResourceTripleInMemScan::name, functions) :
                      BuiltInFunctionsUtils::matchFunction(clientContext->getTx(),
                          RdfResourceTripleScan::name, functions);
    auto rrrScanFunc = ku_dynamic_cast<Function*, TableFunction*>(func);
    auto rrrColumns = expression_vector{s, p, o};
    auto rrrFileScanInfo = BoundFileScanInfo(*rrrScanFunc, bindData->copy(), rrrColumns);
    auto rrrSource = std::make_unique<BoundFileScanSource>(std::move(rrrFileScanInfo));
    auto rrrTableID = rdfGraphEntry->getResourceTripleTableID();
    auto rrrEntry = catalog->getTableCatalogEntry(clientContext->getTx(), rrrTableID);
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
        BoundCopyFromInfo(rrrEntry, std::move(rrrSource), offset, std::move(rrrExtraInfo));
    // Bind copy literal triples
    func = inMemory ? BuiltInFunctionsUtils::matchFunction(clientContext->getTx(),
                          RdfLiteralTripleInMemScan::name, functions) :
                      BuiltInFunctionsUtils::matchFunction(clientContext->getTx(),
                          RdfLiteralTripleScan::name, functions);
    auto rrlScanFunc = ku_dynamic_cast<Function*, TableFunction*>(func);
    auto rrlColumns = expression_vector{s, p, oOffset};
    auto rrlFileScanInfo = BoundFileScanInfo(*rrlScanFunc, bindData->copy(), rrlColumns);
    auto rrlSource = std::make_unique<BoundFileScanSource>(std::move(rrlFileScanInfo));
    auto rrlTableID = rdfGraphEntry->getLiteralTripleTableID();
    auto rrlEntry = catalog->getTableCatalogEntry(clientContext->getTx(), rrlTableID);
    auto rrlExtraInfo = std::make_unique<ExtraBoundCopyRelInfo>();
    rrlExtraInfo->infos.push_back(sLookUp.copy());
    rrlExtraInfo->infos.push_back(pLookUp.copy());
    rrlExtraInfo->propertyColumns.push_back(oOffset);
    rrlExtraInfo->fromOffset = sOffset;
    rrlExtraInfo->toOffset = oOffset;
    auto rrLCopyInfo =
        BoundCopyFromInfo(rrlEntry, std::move(rrlSource), offset, std::move(rrlExtraInfo));
    // Bind copy rdf
    auto rdfExtraInfo = std::make_unique<ExtraBoundCopyRdfInfo>(std::move(rCopyInfo),
        std::move(lCopyInfo), std::move(rrrCopyInfo), std::move(rrLCopyInfo));
    std::unique_ptr<BoundBaseScanSource> source;
    if (inMemory) {
        auto fileScanInfo = BoundFileScanInfo(*scanFunc, bindData->copy(), expression_vector{});
        source = std::make_unique<BoundFileScanSource>(std::move(fileScanInfo));
    } else {
        source = std::make_unique<BoundEmptyScanSource>();
    }
    auto rdfCopyInfo =
        BoundCopyFromInfo(rdfGraphEntry, std::move(source), offset, std::move(rdfExtraInfo));
    return std::make_unique<BoundCopyFrom>(std::move(rdfCopyInfo));
}

} // namespace binder
} // namespace kuzu
