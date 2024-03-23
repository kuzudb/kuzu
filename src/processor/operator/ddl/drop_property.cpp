#include "processor/operator/ddl/drop_property.h"

#include "storage/storage_manager.h"

namespace kuzu {
namespace processor {

void DropProperty::executeDDLInternal(ExecutionContext* context) {
    auto catalog = context->clientContext->getCatalog();
    auto storageManager = context->clientContext->getStorageManager();
    auto tableEntry = catalog->getTableCatalogEntry(context->clientContext->getTx(), tableID);
    auto columnID = tableEntry->getColumnID(propertyID);
    catalog->dropProperty(tableID, propertyID);
    if (tableEntry->getTableType() == common::TableType::NODE) {
        auto nodesStats = storageManager->getNodesStatisticsAndDeletedIDs();
        nodesStats->removeMetadataDAHInfo(tableID, columnID);
    } else {
        auto relsStats = storageManager->getRelsStatistics();
        relsStats->removeMetadataDAHInfo(tableID, columnID);
    }
}

} // namespace processor
} // namespace kuzu
