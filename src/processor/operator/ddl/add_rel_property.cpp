#include "processor/operator/ddl/add_rel_property.h"

#include "catalog/rel_table_schema.h"
#include "storage/in_mem_storage_structure/in_mem_column.h"
#include "storage/in_mem_storage_structure/in_mem_lists.h"

using namespace kuzu::catalog;
using namespace kuzu::storage;
using namespace kuzu::common;

namespace kuzu {
namespace processor {

static void createFileForRelColumnPropertyWithDefaultVal(table_id_t relTableID,
    table_id_t boundTableID, RelDataDirection direction, const catalog::Property& property,
    uint8_t* defaultVal, bool isDefaultValNull, StorageManager& storageManager) {
    auto inMemColumn = std::make_unique<InMemColumn>(
        StorageUtils::getRelPropertyColumnFName(storageManager.getDirectory(), relTableID,
            direction, property.getPropertyID(), DBFileType::WAL_VERSION),
        *property.getDataType());
    auto numTuples =
        storageManager.getRelsStore().getRelsStatistics().getNumTuplesForTable(relTableID);
    auto inMemColumnChunk =
        inMemColumn->createInMemColumnChunk(0, numTuples - 1, nullptr /* copyDescription */);
    if (!isDefaultValNull) {
        // TODO(Guodong): Rework this.
        //        inMemColumn->fillWithDefaultVal(defaultVal,
        //            storageManager.getNodesStore().getNodesStatisticsAndDeletedIDs().getNumTuplesForTable(
        //                boundTableID),
        //            property.dataType);
    }
    inMemColumn->flushChunk(inMemColumnChunk.get());
    inMemColumn->saveToFile();
}

static void createFileForRelListsPropertyWithDefaultVal(table_id_t relTableID,
    table_id_t boundTableID, RelDataDirection direction, const catalog::Property& property,
    uint8_t* defaultVal, bool isDefaultValNull, StorageManager& storageManager) {
    // Note: we need the listMetadata to get the num of elements in a large list, and headers to
    // get the num of elements in a small list as well as determine whether a list is large or
    // small. All property lists share the same listHeader which is stored in the adjList.
    auto adjLists = storageManager.getRelsStore().getAdjLists(direction, relTableID);
    auto inMemList = InMemListsFactory::getInMemPropertyLists(
        StorageUtils::getRelPropertyListsFName(storageManager.getDirectory(), relTableID, direction,
            property.getPropertyID(), DBFileType::WAL_VERSION),
        *property.getDataType(),
        storageManager.getRelsStore().getRelsStatistics().getNumTuplesForTable(relTableID),
        nullptr /* copyDescription */);
    auto numNodesInBoundTable =
        storageManager.getNodesStore().getNodesStatisticsAndDeletedIDs().getNumTuplesForTable(
            boundTableID);
    inMemList->initListsMetadataAndAllocatePages(
        numNodesInBoundTable, adjLists->getHeaders().get(), &adjLists->getListsMetadata());
    if (!isDefaultValNull) {
        inMemList->fillWithDefaultVal(
            defaultVal, numNodesInBoundTable, adjLists->getHeaders().get());
    }
    inMemList->saveToFile();
}

static void createFileForRelPropertyWithDefaultVal(catalog::RelTableSchema* tableSchema,
    const catalog::Property& property, uint8_t* defaultVal, bool isDefaultValNull,
    StorageManager& storageManager) {
    for (auto direction : RelDataDirectionUtils::getRelDataDirections()) {
        auto createPropertyFileFunc = tableSchema->isSingleMultiplicityInDirection(direction) ?
                                          createFileForRelColumnPropertyWithDefaultVal :
                                          createFileForRelListsPropertyWithDefaultVal;
        createPropertyFileFunc(tableSchema->tableID, tableSchema->getBoundTableID(direction),
            direction, property, defaultVal, isDefaultValNull, storageManager);
    }
}

void AddRelProperty::executeDDLInternal() {
    catalog->addRelProperty(tableID, propertyName, dataType->copy());
    auto tableSchema =
        reinterpret_cast<RelTableSchema*>(catalog->getWriteVersion()->getTableSchema(tableID));
    auto property = tableSchema->getProperty(tableSchema->getPropertyID(propertyName));
    auto defaultValVector = getDefaultValVector();
    auto posInDefaultValVector = defaultValVector->state->selVector->selectedPositions[0];
    auto defaultVal = defaultValVector->getData() +
                      defaultValVector->getNumBytesPerValue() * posInDefaultValVector;
    createFileForRelPropertyWithDefaultVal(tableSchema, *property, defaultVal,
        defaultValVector->isNull(posInDefaultValVector), storageManager);
}

} // namespace processor
} // namespace kuzu
