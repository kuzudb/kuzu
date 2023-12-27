#include "binder/binder.h"
#include "binder/copy/bound_copy_from.h"
#include "catalog/rdf_graph_schema.h"
#include "common/constants.h"
#include "common/keyword/rdf_keyword.h"
#include "common/types/rdf_variant_type.h"
#include "main/client_context.h"
#include "processor/operator/persistent/reader/rdf/rdf_scan.h"

using namespace kuzu::binder;
using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::function;
using namespace kuzu::processor;

namespace kuzu {
namespace binder {

std::unique_ptr<BoundStatement> Binder::bindCopyRdfFrom(
    const parser::Statement&, std::unique_ptr<ReaderConfig> config, TableSchema* tableSchema) {
    auto rdfSchema = ku_dynamic_cast<TableSchema*, RdfGraphSchema*>(tableSchema);
    auto offset = expressionBinder.createVariableExpression(
        *LogicalType::INT64(), InternalKeyword::ROW_OFFSET);
    auto r = expressionBinder.createVariableExpression(*LogicalType::STRING(), rdf::IRI);
    auto l = expressionBinder.createVariableExpression(*RdfVariantType::getType(), rdf::VAL);
    auto s = expressionBinder.createVariableExpression(*LogicalType::STRING(), rdf::SUBJECT);
    auto p = expressionBinder.createVariableExpression(*LogicalType::STRING(), rdf::PREDICATE);
    auto o = expressionBinder.createVariableExpression(*LogicalType::STRING(), rdf::OBJECT);
    auto sOffset = expressionBinder.createVariableExpression(
        *LogicalType::INT64(), InternalKeyword::SRC_OFFSET);
    auto pOffset = expressionBinder.createVariableExpression(*LogicalType::INT64(), rdf::PID);
    auto oOffset = expressionBinder.createVariableExpression(
        *LogicalType::INT64(), InternalKeyword::DST_OFFSET);
    auto scanFunc = getScanFunction(config->fileType, *config);
    // Bind copy resource.
    auto rScanFunc = scanFunc;
    auto rBindInput = std::make_unique<RdfScanBindInput>();
    rBindInput->mode = RdfReaderMode::RESOURCE;
    rBindInput->config = config->copy();
    auto rBindData =
        rScanFunc->bindFunc(clientContext, rBindInput.get(), (Catalog*)&catalog, storageManager);
    auto rColumns = expression_vector{r};
    auto rScanInfo = std::make_unique<BoundFileScanInfo>(
        rScanFunc, std::move(rBindData), std::move(rColumns), offset);
    auto rTableID = rdfSchema->getResourceTableID();
    auto rSchema = catalog.getTableSchema(clientContext->getTx(), rTableID);
    auto rCopyInfo =
        std::make_unique<BoundCopyFromInfo>(rSchema, std::move(rScanInfo), false, nullptr);
    // Bind copy literal.
    auto lScanFunc = scanFunc;
    auto lBindInput = std::make_unique<RdfScanBindInput>();
    lBindInput->mode = RdfReaderMode::LITERAL;
    lBindInput->config = config->copy();
    auto lBindData =
        lScanFunc->bindFunc(clientContext, lBindInput.get(), (Catalog*)&catalog, storageManager);
    auto lColumns = expression_vector{l};
    auto lScanInfo = std::make_unique<BoundFileScanInfo>(
        lScanFunc, std::move(lBindData), std::move(lColumns), offset);
    auto lTableID = rdfSchema->getLiteralTableID();
    auto lSchema = catalog.getTableSchema(clientContext->getTx(), lTableID);
    auto lCopyInfo =
        std::make_unique<BoundCopyFromInfo>(lSchema, std::move(lScanInfo), true, nullptr);
    // Bind copy resource triples
    auto rrrScanFunc = scanFunc;
    auto rrrBindInput = std::make_unique<RdfScanBindInput>();
    rrrBindInput->mode = RdfReaderMode::RESOURCE_TRIPLE;
    rrrBindInput->config = config->copy();
    auto rrrBindData = rrrScanFunc->bindFunc(
        clientContext, rrrBindInput.get(), (Catalog*)&catalog, storageManager);
    auto rrrColumns = expression_vector{s, p, o};
    auto rrrScanInfo = std::make_unique<BoundFileScanInfo>(
        rrrScanFunc, std::move(rrrBindData), rrrColumns, offset);
    auto rrrTableID = rdfSchema->getResourceTripleTableID();
    auto rrrSchema = catalog.getTableSchema(clientContext->getTx(), rrrTableID);
    auto rrrExtraInfo = std::make_unique<ExtraBoundCopyRelInfo>();
    auto sLookUp = std::make_unique<IndexLookupInfo>(rTableID, sOffset, s, s->getDataType().copy());
    auto pLookUp = std::make_unique<IndexLookupInfo>(rTableID, pOffset, p, p->getDataType().copy());
    auto oLookUp = std::make_unique<IndexLookupInfo>(rTableID, oOffset, o, o->getDataType().copy());
    rrrExtraInfo->infos.push_back(sLookUp->copy());
    rrrExtraInfo->infos.push_back(pLookUp->copy());
    rrrExtraInfo->infos.push_back(oLookUp->copy());
    rrrExtraInfo->fromOffset = sOffset;
    rrrExtraInfo->toOffset = oOffset;
    auto rrrCopyInfo = std::make_unique<BoundCopyFromInfo>(
        rrrSchema, std::move(rrrScanInfo), false, std::move(rrrExtraInfo));
    // Bind copy literal triples
    auto rrlScanFunc = scanFunc;
    auto rrlBindInput = std::make_unique<RdfScanBindInput>();
    rrlBindInput->mode = RdfReaderMode::LITERAL_TRIPLE;
    rrlBindInput->config = config->copy();
    auto rrlBindData = rrlScanFunc->bindFunc(
        clientContext, rrlBindInput.get(), (Catalog*)&catalog, storageManager);
    auto rrlColumns = expression_vector{s, p, oOffset};
    auto rrlScanInfo = std::make_unique<BoundFileScanInfo>(
        rrlScanFunc, std::move(rrlBindData), rrlColumns, offset);
    auto rrlTableID = rdfSchema->getLiteralTripleTableID();
    auto rrlSchema = catalog.getTableSchema(clientContext->getTx(), rrlTableID);
    auto rrlExtraInfo = std::make_unique<ExtraBoundCopyRelInfo>();
    rrlExtraInfo->infos.push_back(sLookUp->copy());
    rrlExtraInfo->infos.push_back(pLookUp->copy());
    rrlExtraInfo->propertyColumns.push_back(oOffset);
    rrlExtraInfo->fromOffset = sOffset;
    rrlExtraInfo->toOffset = oOffset;
    auto rrLCopyInfo = std::make_unique<BoundCopyFromInfo>(
        rrlSchema, std::move(rrlScanInfo), false, std::move(rrlExtraInfo));
    // Bind copy rdf
    auto rdfExtraInfo = std::make_unique<ExtraBoundCopyRdfInfo>();
    rdfExtraInfo->rInfo = std::move(rCopyInfo);
    rdfExtraInfo->lInfo = std::move(lCopyInfo);
    rdfExtraInfo->rrrInfo = std::move(rrrCopyInfo);
    rdfExtraInfo->rrlInfo = std::move(rrLCopyInfo);
    auto rdfCopyInfo =
        std::make_unique<BoundCopyFromInfo>(rdfSchema, nullptr, false, std::move(rdfExtraInfo));
    return std::make_unique<BoundCopyFrom>(std::move(rdfCopyInfo));
}

} // namespace binder
} // namespace kuzu
