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

std::unique_ptr<BoundStatement> Binder::bindCopyRdfNodeFrom(function::TableFunction* copyFunc,
    std::unique_ptr<ReaderConfig> readerConfig, TableSchema* tableSchema) {
    bool containsSerial;
    auto stringType = LogicalType{LogicalTypeID::STRING};
    std::vector<std::string> columnNames;
    std::vector<std::unique_ptr<common::LogicalType>> columnTypes;
    columnNames.push_back(std::string(InternalKeyword::ANONYMOUS));
    if (tableSchema->tableName.ends_with(rdf::RESOURCE_TABLE_SUFFIX)) {
        containsSerial = false;
        columnTypes.push_back(stringType.copy());
        readerConfig->rdfReaderConfig =
            std::make_unique<RdfReaderConfig>(RdfReaderMode::RESOURCE, nullptr /* index */);
    } else {
        KU_ASSERT(tableSchema->tableName.ends_with(rdf::LITERAL_TABLE_SUFFIX));
        containsSerial = true;
        columnTypes.push_back(RdfVariantType::getType());
        readerConfig->rdfReaderConfig =
            std::make_unique<RdfReaderConfig>(RdfReaderMode::LITERAL, nullptr /* index */);
    }
    auto bindInput = std::make_unique<function::ScanTableFuncBindInput>(
        memoryManager, *readerConfig, columnNames, std::move(columnTypes));
    auto bindData =
        copyFunc->bindFunc(clientContext, bindInput.get(), catalog.getReadOnlyVersion());
    expression_vector columns;
    for (auto i = 0u; i < bindData->columnTypes.size(); i++) {
        columns.push_back(createVariable(bindData->columnNames[i], *bindData->columnTypes[i]));
    }
    auto offset = expressionBinder.createVariableExpression(
        LogicalType(LogicalTypeID::INT64), common::InternalKeyword::ANONYMOUS);
    auto boundFileScanInfo = std::make_unique<BoundFileScanInfo>(
        copyFunc, std::move(bindData), columns, std::move(offset), TableType::NODE);
    auto boundCopyFromInfo = std::make_unique<BoundCopyFromInfo>(tableSchema,
        std::move(boundFileScanInfo), containsSerial, std::move(columns), nullptr /* extraInfo */);
    return std::make_unique<BoundCopyFrom>(std::move(boundCopyFromInfo));
}

std::unique_ptr<BoundStatement> Binder::bindCopyRdfRelFrom(function::TableFunction* copyFunc,
    std::unique_ptr<ReaderConfig> readerConfig, TableSchema* tableSchema) {
    auto containsSerial = false;
    std::vector<std::string> columnNames;
    std::vector<std::unique_ptr<common::LogicalType>> columnTypes;
    for (auto i = 0u; i < 3; ++i) {
        auto columnName = std::string(InternalKeyword::ANONYMOUS) + std::to_string(i);
        columnNames.push_back(columnName);
        columnTypes.push_back(std::make_unique<LogicalType>(LogicalTypeID::INT64));
    }
    auto relTableSchema = reinterpret_cast<RelTableSchema*>(tableSchema);
    auto resourceTableID = relTableSchema->getSrcTableID();
    auto index = storageManager->getPKIndex(resourceTableID);
    if (tableSchema->tableName.ends_with(rdf::RESOURCE_TRIPLE_TABLE_SUFFIX)) {
        readerConfig->rdfReaderConfig =
            std::make_unique<RdfReaderConfig>(RdfReaderMode::RESOURCE_TRIPLE, index);
    } else {
        readerConfig->rdfReaderConfig =
            std::make_unique<RdfReaderConfig>(RdfReaderMode::LITERAL_TRIPLE, index);
    }
    auto bindInput = std::make_unique<function::ScanTableFuncBindInput>(
        memoryManager, *readerConfig, columnNames, std::move(columnTypes));
    auto bindData =
        copyFunc->bindFunc(clientContext, bindInput.get(), catalog.getReadOnlyVersion());
    expression_vector columns;
    for (auto i = 0u; i < bindData->columnTypes.size(); i++) {
        columns.push_back(createVariable(bindData->columnNames[i], *bindData->columnTypes[i]));
    }
    auto offset = expressionBinder.createVariableExpression(
        LogicalType(LogicalTypeID::INT64), common::InternalKeyword::ANONYMOUS);
    auto boundFileScanInfo = std::make_unique<BoundFileScanInfo>(
        copyFunc, std::move(bindData), columns, offset, TableType::REL);
    auto extraInfo = std::make_unique<ExtraBoundCopyRdfRelInfo>(columns[0], columns[2]);
    expression_vector columnsToCopy = {columns[0], columns[2], offset, columns[1]};
    auto boundCopyFromInfo =
        std::make_unique<BoundCopyFromInfo>(tableSchema, std::move(boundFileScanInfo),
            containsSerial, std::move(columnsToCopy), std::move(extraInfo));
    return std::make_unique<BoundCopyFrom>(std::move(boundCopyFromInfo));
}

} // namespace binder
} // namespace kuzu
