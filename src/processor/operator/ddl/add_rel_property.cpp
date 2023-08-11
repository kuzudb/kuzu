#include "processor/operator/ddl/add_rel_property.h"

using namespace kuzu::storage;

namespace kuzu {
namespace processor {

void AddRelProperty::executeDDLInternal() {
    catalog->addRelProperty(tableID, propertyName, dataType->copy());
    auto tableSchema = catalog->getWriteVersion()->getRelTableSchema(tableID);
    auto property = tableSchema->getProperty(tableSchema->getPropertyID(propertyName));
    auto defaultValVector = getDefaultValVector();
    auto posInDefaultValVector = defaultValVector->state->selVector->selectedPositions[0];
    auto defaultVal = defaultValVector->getData() +
                      defaultValVector->getNumBytesPerValue() * posInDefaultValVector;
    StorageUtils::createFileForRelPropertyWithDefaultVal(tableSchema, *property, defaultVal,
        defaultValVector->isNull(posInDefaultValVector), storageManager);
}

} // namespace processor
} // namespace kuzu
