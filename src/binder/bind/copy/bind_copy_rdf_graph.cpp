#include "binder/binder.h"
#include "binder/copy/bound_copy_from.h"
#include "catalog/rel_table_schema.h"
#include "common/constants.h"
#include "common/copier_config/rdf_reader_config.h"
#include "common/keyword/rdf_keyword.h"
#include "common/types/rdf_variant_type.h"
#include "function/table_functions/bind_input.h"
#include "main/client_context.h"

using namespace kuzu::binder;
using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::parser;

namespace kuzu {
namespace binder {

std::unique_ptr<BoundStatement> Binder::bindCopyRdfNodeFrom(const Statement& /*statement*/,
    std::unique_ptr<ReaderConfig> config, TableSchema* tableSchema) {
    auto func = getScanFunction(config->fileType, *config);
    bool containsSerial;
    std::vector<std::string> columnNames;
    std::vector<std::unique_ptr<common::LogicalType>> columnTypes;
    columnNames.emplace_back(rdf::IRI);
    RdfReaderMode mode;
    if (tableSchema->tableName.ends_with(rdf::RESOURCE_TABLE_SUFFIX)) {
        containsSerial = false;
        columnTypes.push_back(LogicalType::STRING());
        mode = RdfReaderMode::RESOURCE;
    } else {
        KU_ASSERT(tableSchema->tableName.ends_with(rdf::LITERAL_TABLE_SUFFIX));
        containsSerial = true;
        columnTypes.push_back(RdfVariantType::getType());
        mode = RdfReaderMode::LITERAL;
    }
    config->extraConfig = std::make_unique<RdfReaderConfig>(mode);
    auto bindInput = std::make_unique<function::ScanTableFuncBindInput>(
        memoryManager, *config, columnNames, std::move(columnTypes));
    auto bindData =
        func->bindFunc(clientContext, bindInput.get(), (Catalog*)&catalog, storageManager);
    expression_vector columns;
    for (auto i = 0u; i < bindData->columnTypes.size(); i++) {
        columns.push_back(createVariable(bindData->columnNames[i], *bindData->columnTypes[i]));
    }
    auto offset = expressionBinder.createVariableExpression(
        *LogicalType::INT64(), std::string(InternalKeyword::ROW_OFFSET));
    auto boundFileScanInfo =
        std::make_unique<BoundFileScanInfo>(func, std::move(bindData), columns, std::move(offset));
    auto boundCopyFromInfo = std::make_unique<BoundCopyFromInfo>(
        tableSchema, std::move(boundFileScanInfo), containsSerial, nullptr /* extraInfo */);
    return std::make_unique<BoundCopyFrom>(std::move(boundCopyFromInfo));
}

std::unique_ptr<BoundStatement> Binder::bindCopyRdfRelFrom(const Statement& /*statement*/,
    std::unique_ptr<ReaderConfig> config, TableSchema* tableSchema) {
    // Bind Scan
    auto func = getScanFunction(config->fileType, *config);
    auto containsSerial = false;
    std::vector<std::string> columnNames;
    logical_types_t columnTypes;
    RdfReaderMode mode;
    columnNames.emplace_back(rdf::SUBJECT);
    columnNames.emplace_back(rdf::PREDICATE);
    columnTypes.push_back(LogicalType::STRING());
    columnTypes.push_back(LogicalType::STRING());
    if (tableSchema->tableName.ends_with(rdf::RESOURCE_TRIPLE_TABLE_SUFFIX)) {
        mode = RdfReaderMode::RESOURCE_TRIPLE;
        columnNames.emplace_back(rdf::OBJECT);
        columnTypes.push_back(LogicalType::STRING());
    } else {
        mode = RdfReaderMode::LITERAL_TRIPLE;
        columnNames.emplace_back(InternalKeyword::DST_OFFSET);
        columnTypes.push_back(LogicalType::INT64());
    }
    config->extraConfig = std::make_unique<RdfReaderConfig>(mode);
    auto bindInput = std::make_unique<function::ScanTableFuncBindInput>(
        memoryManager, *config, columnNames, std::move(columnTypes));
    auto bindData =
        func->bindFunc(clientContext, bindInput.get(), (Catalog*)&catalog, storageManager);
    expression_vector columns;
    for (auto i = 0u; i < bindData->columnTypes.size(); i++) {
        columns.push_back(createVariable(bindData->columnNames[i], *bindData->columnTypes[i]));
    }
    auto offset = expressionBinder.createVariableExpression(
        LogicalType(LogicalTypeID::INT64), std::string(InternalKeyword::ROW_OFFSET));
    auto boundFileScanInfo =
        std::make_unique<BoundFileScanInfo>(func, std::move(bindData), columns, offset);
    // Bind Copy
    auto rrrSchema = reinterpret_cast<RelTableSchema*>(tableSchema);
    auto rTableID = rrrSchema->getSrcTableID();
    auto extraInfo = std::make_unique<ExtraBoundCopyRelInfo>();
    auto s = columns[0];
    auto p = columns[1];
    auto sOffset = createVariable(InternalKeyword::SRC_OFFSET, LogicalTypeID::INT64);
    auto pOffset = createVariable(rdf::PID, LogicalTypeID::INT64);
    auto sLookUpInfo =
        std::make_unique<IndexLookupInfo>(rTableID, sOffset, s, s->getDataType().copy());
    auto pLookUpInfo =
        std::make_unique<IndexLookupInfo>(rTableID, pOffset, p, p->getDataType().copy());
    extraInfo->infos.push_back(std::move(sLookUpInfo));
    extraInfo->infos.push_back(std::move(pLookUpInfo));
    if (mode == RdfReaderMode::RESOURCE_TRIPLE) {
        auto o = columns[2];
        auto oOffset = createVariable(InternalKeyword::DST_OFFSET, LogicalTypeID::INT64);
        auto oLookUpInfo =
            std::make_unique<IndexLookupInfo>(rTableID, oOffset, o, o->getDataType().copy());
        extraInfo->infos.push_back(std::move(oLookUpInfo));
        extraInfo->toOffset = oOffset;
    } else {
        extraInfo->toOffset = columns[2];
        // This is a temporary hack to make sure object offset will not be projected out.
        extraInfo->propertyColumns.push_back(columns[2]);
    }
    extraInfo->fromOffset = sOffset;
    auto boundCopyFromInfo = std::make_unique<BoundCopyFromInfo>(
        tableSchema, std::move(boundFileScanInfo), containsSerial, std::move(extraInfo));
    return std::make_unique<BoundCopyFrom>(std::move(boundCopyFromInfo));
}

} // namespace binder
} // namespace kuzu
