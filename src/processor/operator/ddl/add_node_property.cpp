#include "processor/operator/ddl/add_node_property.h"

#include "catalog/catalog.h"
#include "storage/storage_manager.h"

namespace kuzu {
namespace processor {

void AddNodeProperty::executeDDLInternal(ExecutionContext* context) {
    auto catalog = context->clientContext->getCatalog();
    auto storageManager = context->clientContext->getStorageManager();
    catalog->addNodeProperty(tableID, propertyName, std::move(dataType));
    auto schema = catalog->getTableCatalogEntry(context->clientContext->getTx(), tableID);
    auto addedPropID = schema->getPropertyID(propertyName);
    auto addedProp = schema->getProperty(addedPropID);
    storageManager->getTable(tableID)->addColumn(
        context->clientContext->getTx(), *addedProp, getDefaultValVector(context));
    storageManager->getWAL()->logAddPropertyRecord(tableID, addedProp->getPropertyID());
}

} // namespace processor
} // namespace kuzu
