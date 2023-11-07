#include "processor/operator/ddl/drop_property.h"

namespace kuzu {
namespace processor {

void DropProperty::executeDDLInternal() {
    auto tableSchema = catalog->getReadOnlyVersion()->getTableSchema(tableID);
    catalog->dropProperty(tableID, propertyID);
    if (tableSchema->tableType == common::TableType::NODE) {
        auto nodesStats = storageManager.getNodesStatisticsAndDeletedIDs();
        nodesStats->removeMetadataDAHInfo(tableID, tableSchema->getColumnID(propertyID));
    } else {
        auto relsStats = storageManager.getRelsStatistics();
        relsStats->removeMetadataDAHInfo(tableID, tableSchema->getColumnID(propertyID));
    }
}

} // namespace processor
} // namespace kuzu
