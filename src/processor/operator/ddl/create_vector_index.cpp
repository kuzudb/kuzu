#include "processor/operator/ddl/create_vector_index.h"

#include "binder/ddl/bound_create_table_info.h"
#include "catalog/catalog.h"
#include "common/string_format.h"
#include "storage/index/vector_index_header.h"
#include "storage/storage_manager.h"

using namespace kuzu::storage;
using namespace kuzu::common;

namespace kuzu {
namespace processor {

void CreateVectorIndex::executeDDLInternal(ExecutionContext* context) {
    auto catalog = context->clientContext->getCatalog();
    auto storageManager = context->clientContext->getStorageManager();

    // Add rel table
    std::vector<binder::PropertyInfo> propertyInfos;
    propertyInfos.emplace_back(InternalKeyword::ID, LogicalType::INTERNAL_ID());
    binder::BoundCreateTableInfo createTableInfo(common::TableType::REL,
        VectorIndexHeader::getIndexRelTableName(tableId, propertyId),
        ConflictAction::ON_CONFLICT_THROW,
        std::make_unique<binder::BoundExtraCreateRelTableInfo>(RelMultiplicity::MANY,
            RelMultiplicity::MANY, tableId, tableId, std::move(propertyInfos)));
    auto relTableId = catalog->createTableSchema(context->clientContext->getTx(), createTableInfo);
    storageManager->createTable(relTableId, catalog, context->clientContext);

    // Add column to table for compressed vectors
    auto table = storageManager->getTable(tableId);
    auto compressedPropertyName = VectorIndexHeader::getCompressedVectorPropertyName(propertyId);
    // Here we do not have any default value evaluator
    auto type = LogicalType::ARRAY(LogicalType::INT8(), dim);
    auto defaultValue =
        std::make_unique<parser::ParsedLiteralExpression>(Value::createNullValue(type), "NULL");
    binder::BoundAlterInfo alterInfo(AlterType::ADD_PROPERTY, tableName, tableId,
        std::make_unique<binder::BoundExtraAddPropertyInfo>(compressedPropertyName,
            LogicalType::ARRAY(LogicalType::INT8(), dim), std::move(defaultValue), nullptr));
    catalog->alterTableSchema(context->clientContext->getTx(), alterInfo);
    auto schema = catalog->getTableCatalogEntry(context->clientContext->getTx(), tableId);
    auto addedPropId = schema->getPropertyID(compressedPropertyName);
    auto addedProp = schema->getProperty(addedPropId);
    table->addColumn(context->clientContext->getTx(), *addedProp, nullptr);
    auto header = std::make_unique<VectorIndexHeader>(dim, config, tableId, propertyId, addedPropId,
        relTableId);
    storageManager->addVectorIndex(std::move(header));
}

std::string CreateVectorIndex::getOutputMsg() {
    return stringFormat("Vector index created on {}.{}", tableName, propertyName);
}

} // namespace processor
} // namespace kuzu
