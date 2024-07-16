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

    std::vector<binder::PropertyInfo> propertyInfos;
    propertyInfos.emplace_back(InternalKeyword::ID, LogicalType::INTERNAL_ID());
    binder::BoundCreateTableInfo createTableInfo(common::TableType::REL,
        VectorIndexHeader::getIndexRelTableName(tableId, propertyId),
        ConflictAction::ON_CONFLICT_THROW,
        std::make_unique<binder::BoundExtraCreateRelTableInfo>(RelMultiplicity::MANY,
            RelMultiplicity::MANY, tableId, tableId, std::move(propertyInfos)));
    auto relTableId = catalog->createTableSchema(context->clientContext->getTx(), createTableInfo);
    storageManager->createTable(relTableId, catalog, context->clientContext);

    auto header = std::make_unique<VectorIndexHeader>(dim, config, tableId, propertyId, relTableId);
    storageManager->addVectorIndex(std::move(header));
}

std::string CreateVectorIndex::getOutputMsg() {
    return stringFormat("Vector index created on {}.{}", tableName, propertyName);
}

} // namespace processor
} // namespace kuzu
