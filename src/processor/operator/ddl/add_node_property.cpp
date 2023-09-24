#include "processor/operator/ddl/add_node_property.h"

namespace kuzu {
namespace processor {

void AddNodeProperty::executeDDLInternal() {
    catalog->addNodeProperty(tableID, propertyName, std::move(dataType));
    auto addedProp = catalog->getWriteVersion()->getNodeProperty(tableID, propertyName);
    storageManager.getNodesStore().getNodeTable(tableID)->addColumn(
        *addedProp, getDefaultValVector(), transaction);
    storageManager.getWAL()->logAddPropertyRecord(tableID, addedProp->getPropertyID());
}

} // namespace processor
} // namespace kuzu
