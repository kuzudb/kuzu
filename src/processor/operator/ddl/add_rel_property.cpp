#include "processor/operator/ddl/add_rel_property.h"

using namespace kuzu::catalog;
using namespace kuzu::storage;
using namespace kuzu::common;

namespace kuzu {
namespace processor {

void AddRelProperty::executeDDLInternal() {
    catalog->addRelProperty(tableID, propertyName, dataType->copy());
    auto tableSchema = catalog->getWriteVersion()->getTableSchema(tableID);
    auto addedPropertyID = tableSchema->getPropertyID(propertyName);
    auto addedProp =
        catalog->getWriteVersion()->getTableSchema(tableID)->getProperty(addedPropertyID);
    storageManager.getRelTable(tableID)->addColumn(transaction, *addedProp, getDefaultValVector());
    storageManager.getWAL()->logAddPropertyRecord(tableID, addedProp->getPropertyID());
}

} // namespace processor
} // namespace kuzu
