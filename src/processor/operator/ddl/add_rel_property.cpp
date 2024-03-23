#include "processor/operator/ddl/add_rel_property.h"

#include "catalog/catalog.h"
#include "storage/storage_manager.h"

using namespace kuzu::catalog;
using namespace kuzu::storage;
using namespace kuzu::common;

namespace kuzu {
namespace processor {

void AddRelProperty::executeDDLInternal(ExecutionContext* context) {
    auto catalog = context->clientContext->getCatalog();
    auto storageManager = context->clientContext->getStorageManager();
    catalog->addRelProperty(tableID, propertyName, dataType->copy());
    auto tableSchema = catalog->getTableCatalogEntry(context->clientContext->getTx(), tableID);
    auto addedPropertyID = tableSchema->getPropertyID(propertyName);
    auto addedProp = tableSchema->getProperty(addedPropertyID);
    storageManager->getRelTable(tableID)->addColumn(
        context->clientContext->getTx(), *addedProp, getDefaultValVector(context));
    storageManager->getWAL()->logAddPropertyRecord(tableID, addedProp->getPropertyID());
}

} // namespace processor
} // namespace kuzu
