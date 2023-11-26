#include "processor/operator/ddl/add_rel_property.h"

using namespace kuzu::catalog;
using namespace kuzu::storage;
using namespace kuzu::common;

namespace kuzu {
namespace processor {

void AddRelProperty::executeDDLInternal(ExecutionContext* context) {
    catalog->addRelProperty(tableID, propertyName, dataType->copy());
    auto tableSchema = catalog->getTableSchema(context->clientContext->getTx(), tableID);
    auto addedPropertyID = tableSchema->getPropertyID(propertyName);
    auto addedProp = tableSchema->getProperty(addedPropertyID);
    storageManager.getRelTable(tableID)->addColumn(transaction, *addedProp, getDefaultValVector());
    storageManager.getWAL()->logAddPropertyRecord(tableID, addedProp->getPropertyID());
}

} // namespace processor
} // namespace kuzu
