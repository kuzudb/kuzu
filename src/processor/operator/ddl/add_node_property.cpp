#include "processor/operator/ddl/add_node_property.h"

namespace kuzu {
namespace processor {

void AddNodeProperty::executeDDLInternal(ExecutionContext* context) {
    catalog->addNodeProperty(tableID, propertyName, std::move(dataType));
    auto schema = catalog->getTableCatalogEntry(context->clientContext->getTx(), tableID);
    auto addedPropID = schema->getPropertyID(propertyName);
    auto addedProp = schema->getProperty(addedPropID);
    storageManager.getNodeTable(tableID)->addColumn(
        transaction, *addedProp, getDefaultValVector(context));
    storageManager.getWAL()->logAddPropertyRecord(tableID, addedProp->getPropertyID());
}

} // namespace processor
} // namespace kuzu
