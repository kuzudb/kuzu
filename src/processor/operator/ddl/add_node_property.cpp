#include "processor/operator/ddl/add_node_property.h"

namespace kuzu {
namespace processor {

void AddNodeProperty::executeDDLInternal() {
    AddProperty::executeDDLInternal();
    auto tableSchema = catalog->getWriteVersion()->getTableSchema(tableID);
    auto property = tableSchema->getProperty(tableSchema->getPropertyID(propertyName));
    StorageUtils::createFileForNodePropertyWithDefaultVal(tableID, storageManager.getDirectory(),
        property, getDefaultVal(), isDefaultValueNull(),
        storageManager.getNodesStore().getNodesStatisticsAndDeletedIDs().getNumTuplesForTable(
            tableID));
    storageManager.getNodesStore().getNodeTable(tableID)->addProperty(property);
}

} // namespace processor
} // namespace kuzu
