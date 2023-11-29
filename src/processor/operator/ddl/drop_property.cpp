#include "processor/operator/ddl/drop_property.h"

namespace kuzu {
namespace processor {

void DropProperty::executeDDLInternal(ExecutionContext* context) {
    auto tableSchema = catalog->getTableSchema(context->clientContext->getTx(), tableID);
    auto columnID = tableSchema->getColumnID(propertyID);
    catalog->dropProperty(tableID, propertyID);
    if (tableSchema->tableType == common::TableType::NODE) {
        auto nodesStats = storageManager.getNodesStatisticsAndDeletedIDs();
        nodesStats->removeMetadataDAHInfo(tableID, columnID);
    } else {
        auto relsStats = storageManager.getRelsStatistics();
        relsStats->removeMetadataDAHInfo(tableID, columnID);
    }
}

} // namespace processor
} // namespace kuzu
