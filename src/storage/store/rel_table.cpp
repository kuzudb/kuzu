#include "storage/store/rel_table.h"

#include "spdlog/spdlog.h"
#include "storage/storage_structure/lists/lists_update_iterator.h"

using namespace kuzu::catalog;

namespace kuzu {
namespace storage {

RelTable::RelTable(const Catalog& catalog, table_id_t tableID, BufferManager& bufferManager,
    MemoryManager& memoryManager, bool isInMemoryMode, WAL* wal)
    : logger{LoggerUtils::getOrCreateLogger("storage")}, tableID{tableID},
      isInMemoryMode{isInMemoryMode}, listsUpdateStore{make_unique<ListsUpdateStore>(memoryManager,
                                          *catalog.getReadOnlyVersion()->getRelTableSchema(
                                              tableID))},
      wal{wal} {
    loadColumnsAndListsFromDisk(catalog, bufferManager);
}

void RelTable::loadColumnsAndListsFromDisk(
    const catalog::Catalog& catalog, BufferManager& bufferManager) {
    initAdjColumnOrLists(catalog, bufferManager, wal);
    initPropertyListsAndColumns(catalog, bufferManager, wal);
}

vector<AdjLists*> RelTable::getAdjListsForNodeTable(table_id_t tableID) {
    vector<AdjLists*> retVal;
    auto it = adjLists[FWD].find(tableID);
    if (it != adjLists[FWD].end()) {
        retVal.push_back(it->second.get());
    }
    it = adjLists[BWD].find(tableID);
    if (it != adjLists[BWD].end()) {
        retVal.push_back(it->second.get());
    }
    return retVal;
}

vector<AdjColumn*> RelTable::getAdjColumnsForNodeTable(table_id_t tableID) {
    vector<AdjColumn*> retVal;
    auto it = adjColumns[FWD].find(tableID);
    if (it != adjColumns[FWD].end()) {
        retVal.push_back(it->second.get());
    }
    it = adjColumns[BWD].find(tableID);
    if (it != adjColumns[BWD].end()) {
        retVal.push_back(it->second.get());
    }
    return retVal;
}

// Prepares all the db file changes necessary to update the "persistent" store of lists with the
// listsUpdateStore, which stores the updates by the write trx locally.
void RelTable::prepareCommitOrRollbackIfNecessary(bool isCommit) {
    auto& listUpdatesPerDirection = listsUpdateStore->getListUpdatesPerTablePerDirection();
    for (auto& relDirection : REL_DIRECTIONS) {
        for (auto& listUpdatesPerTable : listUpdatesPerDirection[relDirection]) {
            if (isCommit && !listUpdatesPerTable.second.empty()) {
                auto srcTableID = listUpdatesPerTable.first;
                auto listsUpdateIterators = getListsUpdateIterators(relDirection, srcTableID);
                // Note: In C++ iterating through maps happens in non-descending order of the keys.
                // This property is critical when using listsUpdateIterator, which requires the user
                // to make calls to writeInMemListToListPages in ascending order of nodeOffsets.
                auto& listUpdatesPerChunk = listUpdatesPerTable.second;
                for (auto updatedChunkItr = listUpdatesPerChunk.begin();
                     updatedChunkItr != listUpdatesPerChunk.end(); ++updatedChunkItr) {
                    for (auto updatedNodeOffsetItr = updatedChunkItr->second.begin();
                         updatedNodeOffsetItr != updatedChunkItr->second.end();
                         updatedNodeOffsetItr++) {
                        auto nodeOffset = updatedNodeOffsetItr->first;
                        auto& listUpdates = updatedNodeOffsetItr->second;
                        // Note: An empty listUpdates can exist for a nodeOffset, because we don't
                        // fix the listUpdates, listUpdatesPerNode and ListUpdatesPerChunk indices
                        // after we insert or delete a rel. For example: a user inserts 1 rel to
                        // nodeOffset1, and then deletes that rel. We will end up getting an empty
                        // listUpdates for nodeOffset1.
                        if (!listUpdates.hasUpdates()) {
                            continue;
                        }
                        if (listUpdates.newlyAddedNode) {
                            listsUpdateIterators[0]->updateList(nodeOffset,
                                *adjLists[relDirection]
                                     .at(srcTableID)
                                     ->getInMemListWithDataFromUpdateStoreOnly(
                                         nodeOffset, listUpdates.insertedRelsTupleIdxInFT));
                            for (auto i = 0u; i < propertyLists[relDirection].at(srcTableID).size();
                                 i++) {
                                listsUpdateIterators[i + 1]->updateList(nodeOffset,
                                    *propertyLists[relDirection]
                                         .at(srcTableID)[i]
                                         ->getInMemListWithDataFromUpdateStoreOnly(
                                             nodeOffset, listUpdates.insertedRelsTupleIdxInFT));
                            }
                        } else if (ListHeaders::isALargeList(adjLists[relDirection]
                                                                 .at(srcTableID)
                                                                 ->getHeaders()
                                                                 ->headersDiskArray->get(nodeOffset,
                                                                     TransactionType::READ_ONLY)) &&
                                   listUpdates.deletedRelIDs.empty()) {
                            // We do an optimization for relPropertyList and adjList :
                            // If the initial list is a largeList and we don't delete any rel from
                            // the persistentStore, we can simply append the newly inserted rels
                            // from the relUpdateStore to largeList. In this case, we can skip
                            // reading the data from persistentStore to InMemList and only need to
                            // read the data from relUpdateStore to InMemList.
                            listsUpdateIterators[0]->appendToLargeList(nodeOffset,
                                *adjLists[relDirection]
                                     .at(srcTableID)
                                     ->getInMemListWithDataFromUpdateStoreOnly(
                                         nodeOffset, listUpdates.insertedRelsTupleIdxInFT));
                            for (auto i = 0u; i < propertyLists[relDirection].at(srcTableID).size();
                                 i++) {
                                listsUpdateIterators[i + 1]->appendToLargeList(nodeOffset,
                                    *propertyLists[relDirection]
                                         .at(srcTableID)[i]
                                         ->getInMemListWithDataFromUpdateStoreOnly(
                                             nodeOffset, listUpdates.insertedRelsTupleIdxInFT));
                            }
                        } else {
                            auto deletedRelOffsetsForList =
                                ((RelIDList*)(propertyLists[relDirection]
                                                  .at(srcTableID)
                                                      [RelTableSchema::INTERNAL_REL_ID_PROPERTY_IDX]
                                                  .get()))
                                    ->getDeletedRelOffsetsInListForNodeOffset(nodeOffset);
                            listsUpdateIterators[0]->updateList(
                                nodeOffset, *adjLists[relDirection]
                                                 .at(srcTableID)
                                                 ->writeToInMemList(nodeOffset,
                                                     listUpdates.insertedRelsTupleIdxInFT,
                                                     deletedRelOffsetsForList));
                            for (auto i = 0u; i < propertyLists[relDirection].at(srcTableID).size();
                                 i++) {
                                listsUpdateIterators[i + 1]->updateList(
                                    nodeOffset, *propertyLists[relDirection]
                                                     .at(srcTableID)[i]
                                                     ->writeToInMemList(nodeOffset,
                                                         listUpdates.insertedRelsTupleIdxInFT,
                                                         deletedRelOffsetsForList));
                            }
                        }
                    }
                    for (auto& listsUpdateIterator : listsUpdateIterators) {
                        listsUpdateIterator->doneUpdating();
                    }
                }
            }
        }
    }
    if (listsUpdateStore->hasUpdates()) {
        addToUpdatedRelTables();
    }
}

void RelTable::checkpointInMemoryIfNecessary() {
    performOpOnListsWithUpdates(
        std::bind(&Lists::checkpointInMemoryIfNecessary, std::placeholders::_1),
        std::bind(&RelTable::clearListsUpdateStore, this));
}

void RelTable::rollbackInMemoryIfNecessary() {
    performOpOnListsWithUpdates(
        std::bind(&Lists::rollbackInMemoryIfNecessary, std::placeholders::_1),
        std::bind(&RelTable::clearListsUpdateStore, this));
}

// This function assumes that the order of vectors in relPropertyVectorsPerRelTable as:
// [relProp1, relProp2, ..., relPropN] and all vectors are flat.
void RelTable::insertRel(const shared_ptr<ValueVector>& srcNodeIDVector,
    const shared_ptr<ValueVector>& dstNodeIDVector,
    const vector<shared_ptr<ValueVector>>& relPropertyVectors) {
    assert(srcNodeIDVector->state->isFlat() && dstNodeIDVector->state->isFlat());
    auto srcTableID =
        srcNodeIDVector->getValue<nodeID_t>(srcNodeIDVector->state->selVector->selectedPositions[0])
            .tableID;
    auto dstTableID =
        dstNodeIDVector->getValue<nodeID_t>(dstNodeIDVector->state->selVector->selectedPositions[0])
            .tableID;
    for (auto direction : REL_DIRECTIONS) {
        auto boundTableID = (direction == RelDirection::FWD ? srcTableID : dstTableID);
        auto boundVector = (direction == RelDirection::FWD ? srcNodeIDVector : dstNodeIDVector);
        auto nbrVector = (direction == RelDirection::FWD ? dstNodeIDVector : srcNodeIDVector);
        if (adjColumns[direction].contains(boundTableID)) {
            auto nodeOffset =
                boundVector->readNodeOffset(boundVector->state->selVector->selectedPositions[0]);
            if (!adjColumns[direction]
                     .at(boundTableID)
                     ->isNull(nodeOffset, Transaction::getDummyWriteTrx().get())) {
                throw RuntimeException(StringUtils::string_format(
                    "RelTable %d is a %s table, but node(nodeOffset: %d, tableID: %d) has "
                    "more than one neighbour in the %s direction.",
                    tableID, inferRelMultiplicity(srcTableID, dstTableID).c_str(), nodeOffset,
                    boundTableID, getRelDirectionAsString(direction).c_str()));
            }
            adjColumns[direction].at(boundTableID)->writeValues(boundVector, nbrVector);
            for (auto i = 0; i < relPropertyVectors.size(); i++) {
                propertyColumns[direction].at(boundTableID)[i]->writeValues(
                    boundVector, relPropertyVectors[i]);
            }
        }
    }
    listsUpdateStore->insertRelIfNecessary(srcNodeIDVector, dstNodeIDVector, relPropertyVectors);
}

void RelTable::deleteRel(const shared_ptr<ValueVector>& srcNodeIDVector,
    const shared_ptr<ValueVector>& dstNodeIDVector, const shared_ptr<ValueVector>& relIDVector) {
    assert(srcNodeIDVector->state->isFlat() && dstNodeIDVector->state->isFlat() &&
           relIDVector->state->isFlat());
    auto srcTableID =
        srcNodeIDVector->getValue<nodeID_t>(srcNodeIDVector->state->selVector->selectedPositions[0])
            .tableID;
    auto dstTableID =
        dstNodeIDVector->getValue<nodeID_t>(dstNodeIDVector->state->selVector->selectedPositions[0])
            .tableID;
    for (auto direction : REL_DIRECTIONS) {
        auto boundTableID = (direction == RelDirection::FWD ? srcTableID : dstTableID);
        auto boundVector = (direction == RelDirection::FWD ? srcNodeIDVector : dstNodeIDVector);
        if (adjColumns[direction].contains(boundTableID)) {
            auto nodeOffset =
                boundVector->readNodeOffset(boundVector->state->selVector->selectedPositions[0]);
            adjColumns[direction].at(boundTableID)->setNodeOffsetToNull(nodeOffset);
            for (auto i = 0; i < propertyColumns[direction].size(); i++) {
                propertyColumns[direction].at(boundTableID)[i]->setNodeOffsetToNull(nodeOffset);
            }
        }
    }
    listsUpdateStore->deleteRelIfNecessary(srcNodeIDVector, dstNodeIDVector, relIDVector);
}

void RelTable::initEmptyRelsForNewNode(nodeID_t& nodeID) {
    for (auto direction : REL_DIRECTIONS) {
        if (adjColumns[direction].contains(nodeID.tableID)) {
            adjColumns[direction].at(nodeID.tableID)->setNodeOffsetToNull(nodeID.offset);
        }
    }
    listsUpdateStore->initNewlyAddedNodes(nodeID);
}

void RelTable::initAdjColumnOrLists(
    const Catalog& catalog, BufferManager& bufferManager, WAL* wal) {
    logger->info("Initializing AdjColumns and AdjLists for rel {}.", tableID);
    adjColumns = vector<table_adj_columns_map_t>{2};
    adjLists = vector<table_adj_lists_map_t>{2};
    for (auto relDirection : REL_DIRECTIONS) {
        for (auto& srcDstTableID :
            catalog.getReadOnlyVersion()->getRelTableSchema(tableID)->getSrcDstTableIDs()) {
            auto boundTableID = relDirection == FWD ? srcDstTableID.first : srcDstTableID.second;
            NodeIDCompressionScheme nodeIDCompressionScheme(
                catalog.getReadOnlyVersion()
                    ->getRelTableSchema(tableID)
                    ->getUniqueNbrTableIDsForBoundTableIDDirection(relDirection, boundTableID));
            if (catalog.getReadOnlyVersion()->isSingleMultiplicityInDirection(
                    tableID, relDirection)) {
                // Add adj column.
                auto adjColumn = make_unique<AdjColumn>(
                    StorageUtils::getAdjColumnStructureIDAndFName(
                        wal->getDirectory(), tableID, boundTableID, relDirection),
                    bufferManager, nodeIDCompressionScheme, isInMemoryMode, wal);
                adjColumns[relDirection].emplace(boundTableID, move(adjColumn));
            } else {
                // Add adj list.
                auto adjList = make_unique<AdjLists>(
                    StorageUtils::getAdjListsStructureIDAndFName(
                        wal->getDirectory(), tableID, boundTableID, relDirection),
                    bufferManager, nodeIDCompressionScheme, isInMemoryMode, wal,
                    listsUpdateStore.get());
                adjLists[relDirection].emplace(boundTableID, move(adjList));
            }
        }
    }
    logger->info("Initializing AdjColumns and AdjLists for rel {} done.", tableID);
}

void RelTable::initPropertyListsAndColumns(
    const Catalog& catalog, BufferManager& bufferManager, WAL* wal) {
    logger->info("Initializing PropertyLists and PropertyColumns for rel {}.", tableID);
    propertyLists = vector<table_property_lists_map_t>{2};
    propertyColumns = vector<table_property_columns_map_t>{2};
    if (!catalog.getReadOnlyVersion()->getRelProperties(tableID).empty()) {
        for (auto relDirection : REL_DIRECTIONS) {
            if (catalog.getReadOnlyVersion()->isSingleMultiplicityInDirection(
                    tableID, relDirection)) {
                initPropertyColumnsForRelTable(catalog, relDirection, bufferManager, wal);
            } else {
                initPropertyListsForRelTable(catalog, relDirection, bufferManager, wal);
            }
        }
    }
    logger->info("Initializing PropertyLists and PropertyColumns for rel {} Done.", tableID);
}

void RelTable::initPropertyColumnsForRelTable(
    const Catalog& catalog, RelDirection relDirection, BufferManager& bufferManager, WAL* wal) {
    logger->debug("Initializing PropertyColumns: relTable {}", tableID);
    for (auto& boundTableID :
        catalog.getReadOnlyVersion()->getNodeTableIDsForRelTableDirection(tableID, relDirection)) {
        auto& properties = catalog.getReadOnlyVersion()->getRelProperties(tableID);
        propertyColumns[relDirection].emplace(
            boundTableID, vector<unique_ptr<Column>>(properties.size()));
        for (auto& property : properties) {
            propertyColumns[relDirection].at(boundTableID)[property.propertyID] =
                ColumnFactory::getColumn(
                    StorageUtils::getRelPropertyColumnStructureIDAndFName(wal->getDirectory(),
                        tableID, boundTableID, relDirection, property.propertyID),
                    property.dataType, bufferManager, isInMemoryMode, wal);
        }
    }
    logger->debug("Initializing PropertyColumns done.");
}

void RelTable::initPropertyListsForRelTable(
    const Catalog& catalog, RelDirection relDirection, BufferManager& bufferManager, WAL* wal) {
    logger->debug("Initializing PropertyLists for rel {}", tableID);
    for (auto& boundTableID :
        catalog.getReadOnlyVersion()->getNodeTableIDsForRelTableDirection(tableID, relDirection)) {
        auto& properties = catalog.getReadOnlyVersion()->getRelProperties(tableID);
        auto adjListsHeaders = adjLists[relDirection].at(boundTableID)->getHeaders();
        propertyLists[relDirection].emplace(
            boundTableID, vector<unique_ptr<Lists>>(properties.size()));
        for (auto& property : properties) {
            auto propertyID = property.propertyID;
            propertyLists[relDirection].at(boundTableID)[property.propertyID] =
                ListsFactory::getLists(
                    StorageUtils::getRelPropertyListsStructureIDAndFName(
                        wal->getDirectory(), tableID, boundTableID, relDirection, property),
                    property.dataType, adjListsHeaders, bufferManager, isInMemoryMode, wal,
                    listsUpdateStore.get());
        }
    }
    logger->debug("Initializing PropertyLists for rel {} done.", tableID);
}

void RelTable::performOpOnListsWithUpdates(
    std::function<void(Lists*)> opOnListsWithUpdates, std::function<void()> opIfHasUpdates) {
    auto& listUpdatesPerDirection = listsUpdateStore->getListUpdatesPerTablePerDirection();
    for (auto& relDirection : REL_DIRECTIONS) {
        for (auto& listUpdatesPerTable : listUpdatesPerDirection[relDirection]) {
            if (!listUpdatesPerTable.second.empty()) {
                auto tableID = listUpdatesPerTable.first;
                opOnListsWithUpdates(adjLists[relDirection].at(tableID).get());
                for (auto& propertyList : propertyLists[relDirection].at(tableID)) {
                    opOnListsWithUpdates(propertyList.get());
                }
            }
        }
    }
    if (listsUpdateStore->hasUpdates()) {
        opIfHasUpdates();
    }
}

string RelTable::inferRelMultiplicity(table_id_t srcTableID, table_id_t dstTableID) {
    auto isFWDColumn = adjColumns[RelDirection::FWD].contains(srcTableID);
    auto isBWDColumn = adjColumns[RelDirection::BWD].contains(dstTableID);
    if (isFWDColumn && isBWDColumn) {
        return "ONE_ONE";
    } else if (isFWDColumn && !isBWDColumn) {
        return "MANY_ONE";
    } else if (!isFWDColumn && isBWDColumn) {
        return "ONE_MANY";
    } else {
        return "MANY_MANY";
    }
}

vector<unique_ptr<ListsUpdateIterator>> RelTable::getListsUpdateIterators(
    RelDirection relDirection, table_id_t srcTableID) const {
    vector<unique_ptr<ListsUpdateIterator>> listsUpdateIterators;
    listsUpdateIterators.push_back(ListsUpdateIteratorFactory::getListsUpdateIterator(
        adjLists[relDirection].at(srcTableID).get()));
    for (auto& propList : propertyLists[relDirection].at(srcTableID)) {
        listsUpdateIterators.push_back(
            ListsUpdateIteratorFactory::getListsUpdateIterator(propList.get()));
    }
    return listsUpdateIterators;
}

} // namespace storage
} // namespace kuzu
