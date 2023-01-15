#include "processor/operator/ddl/add_rel_property.h"

namespace kuzu {
namespace processor {

void AddRelProperty::executeDDLInternal() {
    AddProperty::executeDDLInternal();
    auto tableSchema = catalog->getWriteVersion()->getRelTableSchema(tableID);
    auto propertyID = tableSchema->getPropertyID(propertyName);
    auto property = tableSchema->getProperty(propertyID);
    StorageUtils::createFileForRelPropertyWithDefaultVal(
        tableSchema, property, getDefaultVal(), isDefaultValueNull(), storageManager);
    storageManager.getRelsStore().getRelTable(tableID)->addProperty(property, tableID);
}

} // namespace processor
} // namespace kuzu
