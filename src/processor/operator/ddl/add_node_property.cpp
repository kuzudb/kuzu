#include "processor/operator/ddl/add_node_property.h"

namespace kuzu {
namespace processor {

void AddNodeProperty::executeDDLInternal() {
    catalog->addNodeProperty(tableID, propertyName, std::move(dataType));
    auto schema = catalog->getWriteVersion()->getTableSchema(tableID);
    auto addedPropID = schema->getPropertyID(propertyName);
    auto addedProp = schema->getProperty(addedPropID);
    storageManager.getNodesStore().getNodeTable(tableID)->addColumn(
        *addedProp, getDefaultValVector(), transaction);
    storageManager.getWAL()->logAddPropertyRecord(tableID, addedProp->getPropertyID());
}

} // namespace processor
} // namespace kuzu
