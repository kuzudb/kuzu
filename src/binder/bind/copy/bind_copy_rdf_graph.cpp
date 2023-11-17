#include "binder/binder.h"
#include "binder/copy/bound_copy_from.h"
#include "catalog/rel_table_schema.h"
#include "common/constants.h"
#include "common/keyword/rdf_keyword.h"
#include "common/types/rdf_variant_type.h"
#include "function/table_functions/bind_input.h"

using namespace kuzu::binder;
using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::parser;

namespace kuzu {
namespace binder {

std::unique_ptr<BoundStatement> Binder::bindCopyRdfNodeFrom(const Statement& /*statement*/,
    std::unique_ptr<ReaderConfig> config, TableSchema* tableSchema) {
    auto func = getScanFunction(config->fileType, config->csvReaderConfig->parallel);
    bool containsSerial;
    auto stringType = LogicalType{LogicalTypeID::STRING};
    std::vector<std::string> columnNames;
    std::vector<std::unique_ptr<common::LogicalType>> columnTypes;
    columnNames.emplace_back(rdf::IRI);
    if (tableSchema->tableName.ends_with(rdf::RESOURCE_TABLE_SUFFIX)) {
        containsSerial = false;
        columnTypes.push_back(stringType.copy());
        config->rdfReaderConfig =
            std::make_unique<RdfReaderConfig>(RdfReaderMode::RESOURCE, nullptr /* index */);
    } else {
        KU_ASSERT(tableSchema->tableName.ends_with(rdf::LITERAL_TABLE_SUFFIX));
        containsSerial = true;
        columnTypes.push_back(RdfVariantType::getType());
        config->rdfReaderConfig =
            std::make_unique<RdfReaderConfig>(RdfReaderMode::LITERAL, nullptr /* index */);
    }
    auto bindInput = std::make_unique<function::ScanTableFuncBindInput>(
        memoryManager, *config, columnNames, std::move(columnTypes));
    auto bindData = func->bindFunc(clientContext, bindInput.get(), catalog.getReadOnlyVersion());
    expression_vector columns;
    for (auto i = 0u; i < bindData->columnTypes.size(); i++) {
        columns.push_back(createVariable(bindData->columnNames[i], *bindData->columnTypes[i]));
    }
    auto offset = expressionBinder.createVariableExpression(
        LogicalType(LogicalTypeID::INT64), InternalKeyword::ROW_OFFSET);
    auto boundFileScanInfo =
        std::make_unique<BoundFileScanInfo>(func, std::move(bindData), columns, std::move(offset));
    auto boundCopyFromInfo = std::make_unique<BoundCopyFromInfo>(
        tableSchema, std::move(boundFileScanInfo), containsSerial, nullptr /* extraInfo */);
    return std::make_unique<BoundCopyFrom>(std::move(boundCopyFromInfo));
}

std::unique_ptr<BoundStatement> Binder::bindCopyRdfRelFrom(const Statement& /*statement*/,
    std::unique_ptr<ReaderConfig> config, TableSchema* tableSchema) {
    auto func = getScanFunction(config->fileType, config->csvReaderConfig->parallel);
    auto containsSerial = false;
    std::vector<std::string> columnNames;
    columnNames.emplace_back(InternalKeyword::SRC_OFFSET);
    columnNames.emplace_back(rdf::PID);
    columnNames.emplace_back(InternalKeyword::DST_OFFSET);
    std::vector<std::unique_ptr<common::LogicalType>> columnTypes;
    for (auto i = 0u; i < 3; ++i) {
        columnTypes.push_back(LogicalType::INT64());
    }
    auto relTableSchema = reinterpret_cast<RelTableSchema*>(tableSchema);
    auto resourceTableID = relTableSchema->getSrcTableID();
    auto index = storageManager->getPKIndex(resourceTableID);
    if (tableSchema->tableName.ends_with(rdf::RESOURCE_TRIPLE_TABLE_SUFFIX)) {
        config->rdfReaderConfig =
            std::make_unique<RdfReaderConfig>(RdfReaderMode::RESOURCE_TRIPLE, index);
    } else {
        config->rdfReaderConfig =
            std::make_unique<RdfReaderConfig>(RdfReaderMode::LITERAL_TRIPLE, index);
    }
    auto bindInput = std::make_unique<function::ScanTableFuncBindInput>(
        memoryManager, *config, columnNames, std::move(columnTypes));
    auto bindData = func->bindFunc(clientContext, bindInput.get(), catalog.getReadOnlyVersion());
    expression_vector columns;
    for (auto i = 0u; i < bindData->columnTypes.size(); i++) {
        columns.push_back(createVariable(bindData->columnNames[i], *bindData->columnTypes[i]));
    }
    auto offset = expressionBinder.createVariableExpression(
        LogicalType(LogicalTypeID::INT64), InternalKeyword::ROW_OFFSET);
    auto boundFileScanInfo =
        std::make_unique<BoundFileScanInfo>(func, std::move(bindData), columns, offset);
    auto extraInfo = std::make_unique<ExtraBoundCopyRdfRelInfo>(columns[0], columns[2]);
    expression_vector columnsToCopy = {columns[0], columns[2], offset, columns[1]};
    auto boundCopyFromInfo = std::make_unique<BoundCopyFromInfo>(
        tableSchema, std::move(boundFileScanInfo), containsSerial, std::move(extraInfo));
    return std::make_unique<BoundCopyFrom>(std::move(boundCopyFromInfo));
}

} // namespace binder
} // namespace kuzu
