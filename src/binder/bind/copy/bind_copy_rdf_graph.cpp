#include "binder/binder.h"
#include "binder/copy/bound_copy_from.h"
#include "catalog/rel_table_schema.h"
#include "common/constants.h"
#include "common/keyword/rdf_keyword.h"
#include "common/types/rdf_variant_type.h"
#include "storage/storage_manager.h"

using namespace kuzu::binder;
using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::parser;

namespace kuzu {
namespace binder {

std::unique_ptr<BoundStatement> Binder::bindCopyRdfNodeFrom(
    std::unique_ptr<ReaderConfig> readerConfig, TableSchema* tableSchema) {
    bool containsSerial;
    auto stringType = LogicalType{LogicalTypeID::STRING};
    auto nodeID = createVariable(std::string(Property::INTERNAL_ID_NAME), LogicalTypeID::INT64);
    expression_vector columns;
    auto columnName = std::string(InternalKeyword::ANONYMOUS);
    readerConfig->columnNames.push_back(columnName);
    if (tableSchema->tableName.ends_with(rdf::RESOURCE_TABLE_SUFFIX)) {
        containsSerial = false;
        readerConfig->columnTypes.push_back(stringType.copy());
        readerConfig->rdfReaderConfig =
            std::make_unique<RdfReaderConfig>(RdfReaderMode::RESOURCE, nullptr /* index */);
        columns.push_back(createVariable(columnName, stringType));
    } else {
        assert(tableSchema->tableName.ends_with(rdf::LITERAL_TABLE_SUFFIX));
        containsSerial = true;
        readerConfig->columnTypes.push_back(RdfVariantType::getType());
        readerConfig->rdfReaderConfig =
            std::make_unique<RdfReaderConfig>(RdfReaderMode::LITERAL, nullptr /* index */);
        columns.push_back(createVariable(columnName, *RdfVariantType::getType()));
    }
    auto boundFileScanInfo = std::make_unique<BoundFileScanInfo>(
        std::move(readerConfig), columns, std::move(nodeID), TableType::NODE);
    auto boundCopyFromInfo = std::make_unique<BoundCopyFromInfo>(tableSchema,
        std::move(boundFileScanInfo), containsSerial, std::move(columns), nullptr /* extraInfo */);
    return std::make_unique<BoundCopyFrom>(std::move(boundCopyFromInfo));
}

std::unique_ptr<BoundStatement> Binder::bindCopyRdfRelFrom(
    std::unique_ptr<ReaderConfig> readerConfig, TableSchema* tableSchema) {
    auto containsSerial = false;
    auto offsetType = std::make_unique<LogicalType>(LogicalTypeID::INT64);
    expression_vector columns;
    for (auto i = 0u; i < 3; ++i) {
        auto columnName = std::string(InternalKeyword::ANONYMOUS) + std::to_string(i);
        readerConfig->columnNames.push_back(columnName);
        readerConfig->columnTypes.push_back(offsetType->copy());
        columns.push_back(createVariable(columnName, *offsetType));
    }
    auto relTableSchema = reinterpret_cast<RelTableSchema*>(tableSchema);
    auto resourceTableID = relTableSchema->getSrcTableID();
    auto index = storageManager->getNodesStore().getPKIndex(resourceTableID);
    if (tableSchema->tableName.ends_with(rdf::RESOURCE_TRIPLE_TABLE_SUFFIX)) {
        readerConfig->rdfReaderConfig =
            std::make_unique<RdfReaderConfig>(RdfReaderMode::RESOURCE_TRIPLE, index);
    } else {
        readerConfig->rdfReaderConfig =
            std::make_unique<RdfReaderConfig>(RdfReaderMode::LITERAL_TRIPLE, index);
    }
    auto relID = createVariable(std::string(Property::INTERNAL_ID_NAME), LogicalTypeID::INT64);
    auto boundFileScanInfo = std::make_unique<BoundFileScanInfo>(
        std::move(readerConfig), columns, relID, TableType::REL);
    auto extraInfo = std::make_unique<ExtraBoundCopyRdfRelInfo>(columns[0], columns[1], columns[2]);
    columns.push_back(std::move(relID));
    auto boundCopyFromInfo = std::make_unique<BoundCopyFromInfo>(tableSchema,
        std::move(boundFileScanInfo), containsSerial, std::move(columns), std::move(extraInfo));
    return std::make_unique<BoundCopyFrom>(std::move(boundCopyFromInfo));
}

} // namespace binder
} // namespace kuzu
