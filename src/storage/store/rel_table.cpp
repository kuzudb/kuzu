#include "storage/store/rel_table.h"

#include "spdlog/spdlog.h"
#include "storage/storage_structure/lists/lists_update_iterator.h"

using namespace kuzu::catalog;

namespace kuzu {
namespace storage {

void ListsUpdateIteratorsForDirection::addListUpdateIteratorForAdjList(
    unique_ptr<ListsUpdateIterator> listsUpdateIterator) {
    listUpdateIteratorForAdjList = std::move(listsUpdateIterator);
}

void ListsUpdateIteratorsForDirection::addListUpdateIteratorForPropertyList(
    property_id_t propertyID, unique_ptr<ListsUpdateIterator> listsUpdateIterator) {
    listUpdateIteratorsForPropertyLists.emplace(propertyID, std::move(listsUpdateIterator));
}

void ListsUpdateIteratorsForDirection::doneUpdating() {
    listUpdateIteratorForAdjList->doneUpdating();
    for (auto& [_, listUpdateIteratorForPropertyList] : listUpdateIteratorsForPropertyLists) {
        listUpdateIteratorForPropertyList->doneUpdating();
    }
}

Column* DirectedRelTableData::getPropertyColumn(
    table_id_t boundNodeTableID, property_id_t propertyID) {
    if (propertyColumns.contains(boundNodeTableID) &&
        propertyColumns.at(boundNodeTableID).contains(propertyID)) {
        return propertyColumns.at(boundNodeTableID).at(propertyID).get();
    }
    return nullptr;
}

Lists* DirectedRelTableData::getPropertyLists(
    table_id_t boundNodeTableID, property_id_t propertyID) {
    if (propertyLists.contains(boundNodeTableID) &&
        propertyLists.at(boundNodeTableID).contains(propertyID)) {
        return propertyLists.at(boundNodeTableID).at(propertyID).get();
    }
    return nullptr;
}

AdjColumn* DirectedRelTableData::getAdjColumn(table_id_t boundNodeTableID) {
    if (adjColumns.contains(boundNodeTableID)) {
        return adjColumns.at(boundNodeTableID).get();
    }
    return nullptr;
}

AdjLists* DirectedRelTableData::getAdjLists(table_id_t boundNodeTableID) {
    if (adjLists.contains(boundNodeTableID)) {
        return adjLists.at(boundNodeTableID).get();
    }
    return nullptr;
}

void DirectedRelTableData::initializeData(RelTableSchema* tableSchema, WAL* wal) {
    for (auto& [srcTableID, dstTableID] : tableSchema->getSrcDstTableIDs()) {
        auto boundNodeTableID = direction == FWD ? srcTableID : dstTableID;
        NodeIDCompressionScheme nodeIDCompressionScheme(
            tableSchema->getUniqueNbrTableIDsForBoundTableIDDirection(direction, boundNodeTableID));
        if (tableSchema->isSingleMultiplicityInDirection(direction)) {
            initializeColumnsForBoundNodeTable(
                tableSchema, boundNodeTableID, nodeIDCompressionScheme, bufferManager, wal);
        } else {
            initializeListsForBoundNodeTabl(
                tableSchema, boundNodeTableID, nodeIDCompressionScheme, bufferManager, wal);
        }
    }
}

void DirectedRelTableData::initializeColumnsForBoundNodeTable(RelTableSchema* tableSchema,
    table_id_t boundNodeTableID, NodeIDCompressionScheme& nodeIDCompressionScheme,
    BufferManager& bufferManager, WAL* wal) {
    adjColumns[boundNodeTableID] =
        make_unique<AdjColumn>(StorageUtils::getAdjColumnStructureIDAndFName(wal->getDirectory(),
                                   tableSchema->tableID, boundNodeTableID, direction),
            bufferManager, nodeIDCompressionScheme, isInMemoryMode, wal);
    for (auto& property : tableSchema->properties) {
        propertyColumns[boundNodeTableID][property.propertyID] = ColumnFactory::getColumn(
            StorageUtils::getRelPropertyColumnStructureIDAndFName(wal->getDirectory(),
                tableSchema->tableID, boundNodeTableID, direction, property.propertyID),
            property.dataType, bufferManager, isInMemoryMode, wal);
    }
}

void DirectedRelTableData::initializeListsForBoundNodeTabl(RelTableSchema* tableSchema,
    table_id_t boundNodeTableID, NodeIDCompressionScheme& nodeIDCompressionScheme,
    BufferManager& bufferManager, WAL* wal) {
    adjLists[boundNodeTableID] =
        make_unique<AdjLists>(StorageUtils::getAdjListsStructureIDAndFName(wal->getDirectory(),
                                  tableSchema->tableID, boundNodeTableID, direction),
            bufferManager, nodeIDCompressionScheme, isInMemoryMode, wal, listsUpdatesStore);
    for (auto& property : tableSchema->properties) {
        propertyLists[boundNodeTableID][property.propertyID] = ListsFactory::getLists(
            StorageUtils::getRelPropertyListsStructureIDAndFName(
                wal->getDirectory(), tableSchema->tableID, boundNodeTableID, direction, property),
            property.dataType, adjLists[boundNodeTableID]->getHeaders(), bufferManager,
            isInMemoryMode, wal, listsUpdatesStore);
    }
}

void DirectedRelTableData::scanColumns(Transaction* transaction, RelTableScanState& scanState,
    const shared_ptr<ValueVector>& inNodeIDVector, vector<shared_ptr<ValueVector>>& outputVectors) {
    auto adjColumn = adjColumns.at(scanState.boundNodeTableID).get();
    // Note: The scan operator should guarantee that the first property in the output is adj column.
    adjColumn->read(transaction, inNodeIDVector, outputVectors[0]);
    NodeIDVector::discardNull(*outputVectors[0]);
    if (outputVectors[0]->state->selVector->selectedSize == 0) {
        return;
    }
    for (auto i = 0u; i < scanState.propertyIds.size(); i++) {
        auto propertyId = scanState.propertyIds[i];
        auto outputVectorId = i + 1;
        if (propertyId == INVALID_PROPERTY_ID) {
            outputVectors[outputVectorId]->setAllNull();
            continue;
        }
        auto propertyColumn = getPropertyColumn(scanState.boundNodeTableID, propertyId);
        propertyColumn->read(transaction, inNodeIDVector, outputVectors[outputVectorId]);
    }
}

void DirectedRelTableData::scanLists(Transaction* transaction, RelTableScanState& scanState,
    const shared_ptr<ValueVector>& inNodeIDVector, vector<shared_ptr<ValueVector>>& outputVectors) {
    auto adjList = getAdjLists(scanState.boundNodeTableID);
    if (scanState.syncState->isBoundNodeOffsetInValid()) {
        auto currentIdx = inNodeIDVector->state->selVector->selectedPositions[0];
        if (inNodeIDVector->isNull(currentIdx)) {
            outputVectors[0]->state->selVector->selectedSize = 0;
            return;
        }
        auto currentNodeOffset = inNodeIDVector->readNodeOffset(currentIdx);
        adjList->initListReadingState(
            currentNodeOffset, *scanState.listHandles[0], transaction->getType());
    }
    adjList->readValues(transaction, outputVectors[0], *scanState.listHandles[0]);
    for (auto i = 0u; i < scanState.propertyIds.size(); i++) {
        auto propertyId = scanState.propertyIds[i];
        auto outputVectorId = i + 1;
        if (propertyId == INVALID_PROPERTY_ID) {
            outputVectors[outputVectorId]->setAllNull();
            continue;
        }
        auto propertyList = getPropertyLists(scanState.boundNodeTableID, propertyId);
        propertyList->readValues(
            transaction, outputVectors[outputVectorId], *scanState.listHandles[outputVectorId]);
        propertyList->setDeletedRelsIfNecessary(
            transaction, *scanState.listHandles[outputVectorId], outputVectors[outputVectorId]);
    }
}

void DirectedRelTableData::insertRel(const shared_ptr<ValueVector>& boundVector,
    const shared_ptr<ValueVector>& nbrVector,
    const vector<shared_ptr<ValueVector>>& relPropertyVectors) {
    auto boundTableID =
        boundVector->getValue<nodeID_t>(boundVector->state->selVector->selectedPositions[0])
            .tableID;
    if (!adjColumns.contains(boundTableID)) {
        return;
    }
    auto adjColumn = adjColumns.at(boundTableID).get();
    auto nodeOffset =
        boundVector->readNodeOffset(boundVector->state->selVector->selectedPositions[0]);
    // TODO(Guodong): We should pass a write transaction pointer down.
    if (!adjColumn->isNull(nodeOffset, Transaction::getDummyWriteTrx().get())) {
        throw RuntimeException(
            StringUtils::string_format("Node(nodeOffset: %d, tableID: %d) in RelTable %d cannot "
                                       "have more than one neighbour in the %s direction.",
                nodeOffset, boundTableID, tableID, getRelDirectionAsString(direction).c_str()));
    }
    adjColumn->writeValues(boundVector, nbrVector);
    for (auto i = 0u; i < relPropertyVectors.size(); i++) {
        auto propertyColumn = getPropertyColumn(boundTableID, i);
        propertyColumn->writeValues(boundVector, relPropertyVectors[i]);
    }
}

void DirectedRelTableData::deleteRel(const shared_ptr<ValueVector>& boundVector) {
    auto boundNode =
        boundVector->getValue<nodeID_t>(boundVector->state->selVector->selectedPositions[0]);
    if (!adjColumns.contains(boundNode.tableID)) {
        return;
    }
    auto adjColumn = adjColumns.at(boundNode.tableID).get();
    auto nodeOffset =
        boundVector->readNodeOffset(boundVector->state->selVector->selectedPositions[0]);
    adjColumn->setNodeOffsetToNull(nodeOffset);
    for (auto& [_, propertyColumn] : propertyColumns.at(boundNode.tableID)) {
        propertyColumn->setNodeOffsetToNull(nodeOffset);
    }
}

void DirectedRelTableData::updateRel(const shared_ptr<ValueVector>& boundVector,
    property_id_t propertyID, const shared_ptr<ValueVector>& propertyVector) {
    auto boundNode =
        boundVector->getValue<nodeID_t>(boundVector->state->selVector->selectedPositions[0]);
    if (!adjColumns.contains(boundNode.tableID)) {
        return;
    }
    propertyColumns.at(boundNode.tableID).at(propertyID)->writeValues(boundVector, propertyVector);
}

void DirectedRelTableData::performOpOnListsWithUpdates(
    const std::function<void(Lists*)>& opOnListsWithUpdates) {
    for (auto& [boundNodeTableID, listsUpdatePerTable] :
        listsUpdatesStore->getListsUpdatesPerBoundNodeTableOfDirection(direction)) {
        opOnListsWithUpdates(adjLists.at(boundNodeTableID).get());
        for (auto& [propertyID, propertyList] : propertyLists.at(boundNodeTableID)) {
            opOnListsWithUpdates(propertyList.get());
        }
    }
}

unique_ptr<ListsUpdateIteratorsForDirection>
DirectedRelTableData::getListsUpdateIteratorsForDirection(table_id_t boundNodeTableID) {
    unique_ptr<ListsUpdateIteratorsForDirection> listsUpdateIteratorsForDirection =
        make_unique<ListsUpdateIteratorsForDirection>();
    listsUpdateIteratorsForDirection->addListUpdateIteratorForAdjList(
        ListsUpdateIteratorFactory::getListsUpdateIterator(adjLists.at(boundNodeTableID).get()));
    for (auto& [propertyID, propertyList] : propertyLists.at(boundNodeTableID)) {
        listsUpdateIteratorsForDirection->addListUpdateIteratorForPropertyList(
            propertyID, ListsUpdateIteratorFactory::getListsUpdateIterator(propertyList.get()));
    }
    return listsUpdateIteratorsForDirection;
}

RelTable::RelTable(const Catalog& catalog, table_id_t tableID, BufferManager& bufferManager,
    MemoryManager& memoryManager, bool isInMemoryMode, WAL* wal)
    : tableID{tableID}, wal{wal} {
    auto tableSchema = catalog.getReadOnlyVersion()->getRelTableSchema(tableID);
    listsUpdatesStore = make_unique<ListsUpdatesStore>(memoryManager, *tableSchema);
    fwdRelTableData = make_unique<DirectedRelTableData>(
        tableID, FWD, listsUpdatesStore.get(), isInMemoryMode, bufferManager);
    bwdRelTableData = make_unique<DirectedRelTableData>(
        tableID, BWD, listsUpdatesStore.get(), isInMemoryMode, bufferManager);
    initializeData(tableSchema);
}

void RelTable::initializeData(RelTableSchema* tableSchema) {
    fwdRelTableData->initializeData(tableSchema, wal);
    bwdRelTableData->initializeData(tableSchema, wal);
}

vector<AdjLists*> RelTable::getAdjListsForNodeTable(table_id_t boundNodeTableID) {
    vector<AdjLists*> retVal;
    if (fwdRelTableData->hasAdjLists(boundNodeTableID)) {
        retVal.push_back(fwdRelTableData->getAdjLists(boundNodeTableID));
    }
    if (bwdRelTableData->hasAdjLists(boundNodeTableID)) {
        retVal.push_back(bwdRelTableData->getAdjLists(boundNodeTableID));
    }
    return retVal;
}

vector<AdjColumn*> RelTable::getAdjColumnsForNodeTable(table_id_t boundNodeTableID) {
    vector<AdjColumn*> retVal;
    if (fwdRelTableData->hasAdjColumn(boundNodeTableID)) {
        retVal.push_back(fwdRelTableData->getAdjColumn(boundNodeTableID));
    }
    if (bwdRelTableData->hasAdjColumn(boundNodeTableID)) {
        retVal.push_back(bwdRelTableData->getAdjColumn(boundNodeTableID));
    }
    return retVal;
}

// Prepares all the db file changes necessary to update the "persistent" store of lists with the
// listsUpdatesStore, which stores the updates by the write transaction locally.
void RelTable::prepareCommitOrRollbackIfNecessary(bool isCommit) {
    if (isCommit) {
        prepareCommitForDirection(FWD);
        prepareCommitForDirection(BWD);
    }
    if (listsUpdatesStore->hasUpdates()) {
        addToUpdatedRelTables();
    }
}

void RelTable::checkpointInMemoryIfNecessary() {
    performOpOnListsWithUpdates(
        std::bind(&Lists::checkpointInMemoryIfNecessary, std::placeholders::_1),
        std::bind(&RelTable::clearListsUpdatesStore, this));
}

void RelTable::rollbackInMemoryIfNecessary() {
    performOpOnListsWithUpdates(
        std::bind(&Lists::rollbackInMemoryIfNecessary, std::placeholders::_1),
        std::bind(&RelTable::clearListsUpdatesStore, this));
}

// This function assumes that the order of vectors in relPropertyVectorsPerRelTable as:
// [relProp1, relProp2, ..., relPropN] and all vectors are flat.
void RelTable::insertRel(const shared_ptr<ValueVector>& srcNodeIDVector,
    const shared_ptr<ValueVector>& dstNodeIDVector,
    const vector<shared_ptr<ValueVector>>& relPropertyVectors) {
    assert(srcNodeIDVector->state->isFlat() && dstNodeIDVector->state->isFlat());
    fwdRelTableData->insertRel(srcNodeIDVector, dstNodeIDVector, relPropertyVectors);
    bwdRelTableData->insertRel(dstNodeIDVector, srcNodeIDVector, relPropertyVectors);
    listsUpdatesStore->insertRelIfNecessary(srcNodeIDVector, dstNodeIDVector, relPropertyVectors);
}

void RelTable::deleteRel(const shared_ptr<ValueVector>& srcNodeIDVector,
    const shared_ptr<ValueVector>& dstNodeIDVector, const shared_ptr<ValueVector>& relIDVector) {
    assert(srcNodeIDVector->state->isFlat() && dstNodeIDVector->state->isFlat() &&
           relIDVector->state->isFlat());
    fwdRelTableData->deleteRel(srcNodeIDVector);
    bwdRelTableData->deleteRel(dstNodeIDVector);
    listsUpdatesStore->deleteRelIfNecessary(srcNodeIDVector, dstNodeIDVector, relIDVector);
}

void RelTable::updateRel(const shared_ptr<ValueVector>& srcNodeIDVector,
    const shared_ptr<ValueVector>& dstNodeIDVector, const shared_ptr<ValueVector>& relIDVector,
    const shared_ptr<ValueVector>& propertyVector, uint32_t propertyID) {
    assert(srcNodeIDVector->state->isFlat() && dstNodeIDVector->state->isFlat() &&
           relIDVector->state->isFlat() && propertyVector->state->isFlat());
    auto srcNode = srcNodeIDVector->getValue<nodeID_t>(
        srcNodeIDVector->state->selVector->selectedPositions[0]);
    auto dstNode = dstNodeIDVector->getValue<nodeID_t>(
        dstNodeIDVector->state->selVector->selectedPositions[0]);
    fwdRelTableData->updateRel(srcNodeIDVector, propertyID, propertyVector);
    bwdRelTableData->updateRel(dstNodeIDVector, propertyID, propertyVector);
    auto relID =
        relIDVector->getValue<int64_t>(relIDVector->state->selVector->selectedPositions[0]);
    ListsUpdateInfo listsUpdateInfo = ListsUpdateInfo{propertyVector, propertyID, relID,
        fwdRelTableData->getListOffset(srcNode, relID),
        bwdRelTableData->getListOffset(dstNode, relID)};
    listsUpdatesStore->updateRelIfNecessary(srcNodeIDVector, dstNodeIDVector, listsUpdateInfo);
}

void RelTable::initEmptyRelsForNewNode(nodeID_t& nodeID) {
    if (fwdRelTableData->hasAdjColumn(nodeID.tableID)) {
        fwdRelTableData->getAdjColumn(nodeID.tableID)->setNodeOffsetToNull(nodeID.offset);
    }
    if (bwdRelTableData->hasAdjColumn(nodeID.tableID)) {
        bwdRelTableData->getAdjColumn(nodeID.tableID)->setNodeOffsetToNull(nodeID.offset);
    }
    listsUpdatesStore->initNewlyAddedNodes(nodeID);
}

void RelTable::addProperty(Property property, table_id_t tableID) {
    fwdRelTableData->addProperty(property, tableID, wal);
    bwdRelTableData->addProperty(property, tableID, wal);
}

void RelTable::appendInMemListToLargeListOP(
    ListsUpdateIterator* listsUpdateIterator, node_offset_t nodeOffset, InMemList& inMemList) {
    listsUpdateIterator->appendToLargeList(nodeOffset, inMemList);
}

void RelTable::updateListOP(
    ListsUpdateIterator* listsUpdateIterator, node_offset_t nodeOffset, InMemList& inMemList) {
    listsUpdateIterator->updateList(nodeOffset, inMemList);
}

void RelTable::performOpOnListsWithUpdates(const std::function<void(Lists*)>& opOnListsWithUpdates,
    const std::function<void()>& opIfHasUpdates) {
    fwdRelTableData->performOpOnListsWithUpdates(opOnListsWithUpdates);
    bwdRelTableData->performOpOnListsWithUpdates(opOnListsWithUpdates);
    if (listsUpdatesStore->hasUpdates()) {
        opIfHasUpdates();
    }
}

unique_ptr<ListsUpdateIteratorsForDirection> RelTable::getListsUpdateIteratorsForDirection(
    RelDirection relDirection, table_id_t boundNodeTableID) const {
    return relDirection == FWD ?
               fwdRelTableData->getListsUpdateIteratorsForDirection(boundNodeTableID) :
               bwdRelTableData->getListsUpdateIteratorsForDirection(boundNodeTableID);
}

void DirectedRelTableData::removeProperty(property_id_t propertyID) {
    for (auto& [_, propertyColumnsPerBoundTable] : propertyColumns) {
        for (auto& [propertyID_, propertyColumn] : propertyColumnsPerBoundTable) {
            if (propertyID_ == propertyID) {
                propertyColumnsPerBoundTable.erase(propertyID_);
                break;
            }
        }
    }

    for (auto& [_, propertyListsPerBoundTable] : propertyLists) {
        for (auto& [propertyID_, propertyList] : propertyListsPerBoundTable) {
            if (propertyID_ == propertyID) {
                propertyListsPerBoundTable.erase(propertyID_);
                break;
            }
        }
    }
}

void DirectedRelTableData::addProperty(Property& property, table_id_t tableID, WAL* wal) {
    for (auto& [boundTableID, propertyColumnsPerBoundTable] : propertyColumns) {
        propertyColumnsPerBoundTable.emplace(property.propertyID,
            ColumnFactory::getColumn(
                StorageUtils::getRelPropertyColumnStructureIDAndFName(
                    wal->getDirectory(), tableID, boundTableID, direction, property.propertyID),
                property.dataType, bufferManager, isInMemoryMode, wal));
    }

    for (auto& [boundTableID, propertyListsPerBoundTable] : propertyLists) {
        propertyListsPerBoundTable.emplace(property.propertyID,
            ListsFactory::getLists(
                StorageUtils::getRelPropertyListsStructureIDAndFName(
                    wal->getDirectory(), tableID, boundTableID, direction, property),
                property.dataType, adjLists[boundTableID]->getHeaders(), bufferManager,
                isInMemoryMode, wal, listsUpdatesStore));
    }
}

void RelTable::prepareCommitForDirection(RelDirection relDirection) {
    for (auto& [boundNodeTableID, listsUpdatesPerChunk] :
        listsUpdatesStore->getListsUpdatesPerBoundNodeTableOfDirection(relDirection)) {
        if (listsUpdatesPerChunk.empty()) {
            continue;
        }
        auto listsUpdateIteratorsForDirection =
            getListsUpdateIteratorsForDirection(relDirection, boundNodeTableID);
        // Note: call writeInMemListToListPages in ascending order of nodeOffsets is critical here.
        for (auto& [chunkIdx, listsUpdatesPerNode] : listsUpdatesPerChunk) {
            for (auto& [nodeOffset, listsUpdatesForNodeOffset] : listsUpdatesPerNode) {
                // Note: An empty listUpdates can exist for a nodeOffset, because we don't fix the
                // listUpdates, listUpdatesPerNode and ListUpdatesPerChunk indices after we insert
                // or delete a rel. For example: a user inserts 1 rel to nodeOffset1, and then
                // deletes that rel. We will end up getting an empty listsUpdates for nodeOffset1.
                if (!listsUpdatesForNodeOffset->hasUpdates()) {
                    continue;
                }
                auto adjLists = getAdjLists(relDirection, boundNodeTableID);
                if (listsUpdatesForNodeOffset->isNewlyAddedNode) {
                    prepareCommitForListWithUpdateStoreDataOnly(adjLists, nodeOffset,
                        listsUpdatesForNodeOffset.get(), relDirection,
                        listsUpdateIteratorsForDirection.get(), boundNodeTableID, updateListOP);
                    // TODO(Guodong): Do we need to access the header in this way?
                } else if (ListHeaders::isALargeList(adjLists->getHeaders()->headersDiskArray->get(
                               nodeOffset, TransactionType::READ_ONLY)) &&
                           listsUpdatesForNodeOffset->deletedRelIDs.empty() &&
                           !listsUpdatesForNodeOffset->hasUpdates()) {
                    // We do an optimization for relPropertyList and adjList : If the initial list
                    // is a largeList and we didn't delete or update any rel from the
                    // persistentStore, we can simply append the newly inserted rels from the
                    // listsUpdatesStore to largeList. In this case, we can skip reading the data
                    // from persistentStore to InMemList and only need to read the data from
                    // listsUpdatesStore to InMemList.
                    prepareCommitForListWithUpdateStoreDataOnly(adjLists, nodeOffset,
                        listsUpdatesForNodeOffset.get(), relDirection,
                        listsUpdateIteratorsForDirection.get(), boundNodeTableID,
                        appendInMemListToLargeListOP);
                } else {
                    prepareCommitForList(adjLists, nodeOffset, listsUpdatesForNodeOffset.get(),
                        relDirection, listsUpdateIteratorsForDirection.get(), boundNodeTableID);
                }
            }
            listsUpdateIteratorsForDirection->doneUpdating();
        }
    }
}

void RelTable::prepareCommitForListWithUpdateStoreDataOnly(AdjLists* adjLists,
    node_offset_t nodeOffset, ListsUpdatesForNodeOffset* listsUpdatesForNodeOffset,
    RelDirection relDirection, ListsUpdateIteratorsForDirection* listsUpdateIteratorsForDirection,
    table_id_t boundNodeTableID,
    const std::function<void(ListsUpdateIterator* listsUpdateIterator, node_offset_t,
        InMemList& inMemList)>& opOnListsUpdateIterators) {
    auto inMemAdjLists = adjLists->createInMemListWithDataFromUpdateStoreOnly(
        nodeOffset, listsUpdatesForNodeOffset->insertedRelsTupleIdxInFT);
    opOnListsUpdateIterators(listsUpdateIteratorsForDirection->getListUpdateIteratorForAdjList(),
        nodeOffset, *inMemAdjLists);
    for (auto& [propertyID, listUpdateIteratorForPropertyList] :
        listsUpdateIteratorsForDirection->getListsUpdateIteratorsForPropertyLists()) {
        auto inMemPropLists = getPropertyLists(relDirection, boundNodeTableID, propertyID)
                                  ->createInMemListWithDataFromUpdateStoreOnly(nodeOffset,
                                      listsUpdatesForNodeOffset->insertedRelsTupleIdxInFT);
        opOnListsUpdateIterators(
            listUpdateIteratorForPropertyList.get(), nodeOffset, *inMemPropLists);
    }
}

void RelTable::prepareCommitForList(AdjLists* adjLists, node_offset_t nodeOffset,
    ListsUpdatesForNodeOffset* listsUpdatesForNodeOffset, RelDirection relDirection,
    ListsUpdateIteratorsForDirection* listsUpdateIteratorsForDirection,
    table_id_t boundNodeTableID) {
    auto relIDLists = (RelIDList*)getPropertyLists(
        relDirection, boundNodeTableID, RelTableSchema::INTERNAL_REL_ID_PROPERTY_IDX);
    auto deletedRelOffsets = relIDLists->getDeletedRelOffsetsInListForNodeOffset(nodeOffset);
    // Note: updating adjList is not supported, thus updatedPersistentListOffsets
    // for adjList should be empty.
    auto inMemAdjLists =
        adjLists->writeToInMemList(nodeOffset, listsUpdatesForNodeOffset->insertedRelsTupleIdxInFT,
            deletedRelOffsets, nullptr /* updatedPersistentListOffsets */);
    listsUpdateIteratorsForDirection->getListUpdateIteratorForAdjList()->updateList(
        nodeOffset, *inMemAdjLists);
    for (auto& [propertyID, listUpdateIteratorForPropertyList] :
        listsUpdateIteratorsForDirection->getListsUpdateIteratorsForPropertyLists()) {
        auto inMemPropLists =
            getPropertyLists(relDirection, boundNodeTableID, propertyID)
                ->writeToInMemList(nodeOffset, listsUpdatesForNodeOffset->insertedRelsTupleIdxInFT,
                    deletedRelOffsets,
                    &listsUpdatesForNodeOffset->updatedPersistentListOffsets.at(propertyID));
        listUpdateIteratorForPropertyList->updateList(nodeOffset, *inMemPropLists);
    }
}

} // namespace storage
} // namespace kuzu
