#include "processor/operator/ddl/add_rel_property.h"

using namespace kuzu::storage;

namespace kuzu {
namespace processor {

void AddRelProperty::executeDDLInternal() {
    AddProperty::executeDDLInternal();
    auto tableSchema = catalog->getWriteVersion()->getRelTableSchema(tableID);
    auto property = tableSchema->getProperty(tableSchema->getPropertyID(propertyName));
    StorageUtils::createFileForRelPropertyWithDefaultVal(
        tableSchema, property, getDefaultVal(), isDefaultValueNull(), storageManager);
}

} // namespace processor
} // namespace kuzu
