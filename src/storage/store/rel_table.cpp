#include "storage/store/rel_table.h"

#include "common/string_utils.h"
#include "storage/storage_structure/lists/lists_update_iterator.h"

using namespace kuzu::catalog;
using namespace kuzu::common;

namespace kuzu {
namespace storage {

void ListsUpdateIteratorsForDirection::addListUpdateIteratorForAdjList(
    std::unique_ptr<ListsUpdateIterator> listsUpdateIterator) {
    listUpdateIteratorForAdjList = std::move(listsUpdateIterator);
}

void ListsUpdateIteratorsForDirection::addListUpdateIteratorForPropertyList(
    property_id_t propertyID, std::unique_ptr<ListsUpdateIterator> listsUpdateIterator) {
    listUpdateIteratorsForPropertyLists.emplace(propertyID, std::move(listsUpdateIterator));
}

void ListsUpdateIteratorsForDirection::doneUpdating() {
    listUpdateIteratorForAdjList->doneUpdating();
    for (auto& [_, listUpdateIteratorForPropertyList] : listUpdateIteratorsForPropertyLists) {
        listUpdateIteratorForPropertyList->doneUpdating();
    }
}

Column* DirectedRelTableData::getPropertyColumn(property_id_t propertyID) {
    if (propertyColumns.contains(propertyID)) {
        return propertyColumns.at(propertyID).get();
    }
    return nullptr;
}

Lists* DirectedRelTableData::getPropertyLists(property_id_t propertyID) {
    if (propertyLists.contains(propertyID)) {
        return propertyLists.at(propertyID).get();
    }
    return nullptr;
}

void DirectedRelTableData::initializeData(RelTableSchema* tableSchema, WAL* wal) {
    if (isSingleMultiplicity()) {
        initializeColumns(tableSchema, wal);
    } else {
        initializeLists(tableSchema, wal);
    }
}

void DirectedRelTableData::initializeColumns(RelTableSchema* tableSchema, WAL* wal) {
    adjColumn = ColumnFactory::getColumn(StorageUtils::getAdjColumnStructureIDAndFName(
                                             wal->getDirectory(), tableSchema->tableID, direction),
        LogicalType(LogicalTypeID::INTERNAL_ID), &bufferManager, wal);
    for (auto& property : tableSchema->properties) {
        propertyColumns[property.propertyID] = ColumnFactory::getColumn(
            StorageUtils::getRelPropertyColumnStructureIDAndFName(
                wal->getDirectory(), tableSchema->tableID, direction, property.propertyID),
            property.dataType, &bufferManager, wal);
    }
}

void DirectedRelTableData::initializeLists(RelTableSchema* tableSchema, WAL* wal) {
    adjLists = std::make_unique<AdjLists>(StorageUtils::getAdjListsStructureIDAndFName(
                                              wal->getDirectory(), tableSchema->tableID, direction),
        tableSchema->getNbrTableID(direction), &bufferManager, wal, listsUpdatesStore);
    for (auto& property : tableSchema->properties) {
        propertyLists[property.propertyID] = ListsFactory::getLists(
            StorageUtils::getRelPropertyListsStructureIDAndFName(
                wal->getDirectory(), tableSchema->tableID, direction, property),
            property.dataType, adjLists->getHeaders(), &bufferManager, wal, listsUpdatesStore);
    }
}

void DirectedRelTableData::resetColumnsAndLists(
    catalog::RelTableSchema* tableSchema, kuzu::storage::WAL* wal) {
    if (isSingleMultiplicity()) {
        adjColumn.reset();
        for (auto& property : tableSchema->properties) {
            propertyColumns[property.propertyID].reset();
        }
    } else {
        adjLists.reset();
        for (auto& property : tableSchema->properties) {
            propertyLists[property.propertyID].reset();
        }
    }
}

void DirectedRelTableData::scanColumns(transaction::Transaction* transaction,
    RelTableScanState& scanState, common::ValueVector* inNodeIDVector,
    const std::vector<common::ValueVector*>& outputVectors) {
    // Note: The scan operator should guarantee that the first property in the output is adj column.
    adjColumn->read(transaction, inNodeIDVector, outputVectors[0]);
    if (!NodeIDVector::discardNull(*outputVectors[0])) {
        return;
    }
    fillNbrTableIDs(outputVectors[0]);
    for (auto i = 0u; i < scanState.propertyIds.size(); i++) {
        auto propertyId = scanState.propertyIds[i];
        auto outputVectorId = i + 1;
        if (propertyId == INVALID_PROPERTY_ID) {
            outputVectors[outputVectorId]->setAllNull();
            continue;
        }
        auto propertyColumn = getPropertyColumn(propertyId);
        propertyColumn->read(transaction, inNodeIDVector, outputVectors[outputVectorId]);
        if (propertyId == RelTableSchema::INTERNAL_REL_ID_PROPERTY_ID) {
            fillRelTableIDs(outputVectors[outputVectorId]);
        }
    }
}

void DirectedRelTableData::scanLists(transaction::Transaction* transaction,
    RelTableScanState& scanState, ValueVector* inNodeIDVector,
    const std::vector<common::ValueVector*>& outputVectors) {
    if (scanState.syncState->isBoundNodeOffsetInValid()) {
        auto currentIdx = inNodeIDVector->state->selVector->selectedPositions[0];
        if (inNodeIDVector->isNull(currentIdx)) {
            outputVectors[0]->state->selVector->selectedSize = 0;
            return;
        }
        auto currentNodeOffset = inNodeIDVector->readNodeOffset(currentIdx);
        adjLists->initListReadingState(
            currentNodeOffset, *scanState.listHandles[0], transaction->getType());
    }
    adjLists->readValues(transaction, outputVectors[0], *scanState.listHandles[0]);
    for (auto i = 0u; i < scanState.propertyIds.size(); i++) {
        auto propertyId = scanState.propertyIds[i];
        auto outputVectorId = i + 1;
        if (propertyId == INVALID_PROPERTY_ID) {
            outputVectors[outputVectorId]->setAllNull();
            continue;
        }
        auto propertyList = getPropertyLists(propertyId);
        propertyList->readValues(
            transaction, outputVectors[outputVectorId], *scanState.listHandles[outputVectorId]);
        propertyList->setDeletedRelsIfNecessary(
            transaction, *scanState.listHandles[outputVectorId], outputVectors[outputVectorId]);
    }
}

// Fill nbr table IDs for the vector scanned from an adj column.
void DirectedRelTableData::fillNbrTableIDs(common::ValueVector* vector) const {
    assert(vector->dataType.getLogicalTypeID() == LogicalTypeID::INTERNAL_ID);
    auto nodeIDs = (internalID_t*)vector->getData();
    for (auto i = 0u; i < vector->state->selVector->selectedSize; i++) {
        auto pos = vector->state->selVector->selectedPositions[i];
        nodeIDs[pos].tableID = nbrTableID;
    }
}

// Fill rel table IDs for the vector scanned from a RelID column.
void DirectedRelTableData::fillRelTableIDs(common::ValueVector* vector) const {
    auto internalRelIDs = (internalID_t*)vector->getData();
    for (auto i = 0u; i < vector->state->selVector->selectedSize; i++) {
        auto pos = vector->state->selVector->selectedPositions[i];
        internalRelIDs[pos].tableID = tableID;
    }
}

void DirectedRelTableData::insertRel(common::ValueVector* boundVector,
    common::ValueVector* nbrVector, const std::vector<common::ValueVector*>& relPropertyVectors) {
    if (!isSingleMultiplicity()) {
        return;
    }
    auto nodeOffset =
        boundVector->readNodeOffset(boundVector->state->selVector->selectedPositions[0]);
    // TODO(Guodong): We should pass a write transaction pointer down.
    if (!adjColumn->isNull(nodeOffset, transaction::Transaction::getDummyWriteTrx().get())) {
        throw RuntimeException(
            StringUtils::string_format("Node(nodeOffset: {}, tableID: {}) in RelTable {} cannot "
                                       "have more than one neighbour in the {} direction.",
                nodeOffset,
                boundVector->getValue<nodeID_t>(boundVector->state->selVector->selectedPositions[0])
                    .tableID,
                tableID, RelDataDirectionUtils::relDataDirectionToString(direction)));
    }
    adjColumn->write(boundVector, nbrVector);
    for (auto i = 0u; i < relPropertyVectors.size(); i++) {
        auto propertyColumn = getPropertyColumn(i);
        propertyColumn->write(boundVector, relPropertyVectors[i]);
    }
}

void DirectedRelTableData::deleteRel(ValueVector* boundVector) {
    if (!isSingleMultiplicity()) {
        return;
    }
    auto nodeOffset =
        boundVector->readNodeOffset(boundVector->state->selVector->selectedPositions[0]);
    adjColumn->setNull(nodeOffset);
    for (auto& [_, propertyColumn] : propertyColumns) {
        propertyColumn->setNull(nodeOffset);
    }
}

void DirectedRelTableData::updateRel(
    ValueVector* boundVector, property_id_t propertyID, ValueVector* propertyVector) {
    if (!isSingleMultiplicity()) {
        return;
    }
    propertyColumns.at(propertyID)->write(boundVector, propertyVector);
}

void DirectedRelTableData::performOpOnListsWithUpdates(
    const std::function<void(Lists*)>& opOnListsWithUpdates) {
    for (auto& [boundNodeTableID, listsUpdatePerTable] :
        listsUpdatesStore->getListsUpdatesPerChunk(direction)) {
        opOnListsWithUpdates(adjLists.get());
        for (auto& [propertyID, propertyList] : propertyLists) {
            opOnListsWithUpdates(propertyList.get());
        }
    }
}

std::unique_ptr<ListsUpdateIteratorsForDirection>
DirectedRelTableData::getListsUpdateIteratorsForDirection() {
    std::unique_ptr<ListsUpdateIteratorsForDirection> listsUpdateIteratorsForDirection =
        std::make_unique<ListsUpdateIteratorsForDirection>();
    listsUpdateIteratorsForDirection->addListUpdateIteratorForAdjList(
        ListsUpdateIteratorFactory::getListsUpdateIterator(adjLists.get()));
    for (auto& [propertyID, propertyList] : propertyLists) {
        listsUpdateIteratorsForDirection->addListUpdateIteratorForPropertyList(
            propertyID, ListsUpdateIteratorFactory::getListsUpdateIterator(propertyList.get()));
    }
    return listsUpdateIteratorsForDirection;
}

RelTable::RelTable(
    const Catalog& catalog, table_id_t tableID, MemoryManager& memoryManager, WAL* wal)
    : tableID{tableID}, wal{wal} {
    auto tableSchema = catalog.getReadOnlyVersion()->getRelTableSchema(tableID);
    listsUpdatesStore = std::make_unique<ListsUpdatesStore>(memoryManager, *tableSchema);
    fwdRelTableData =
        std::make_unique<DirectedRelTableData>(tableID, tableSchema->getBoundTableID(FWD),
            tableSchema->getNbrTableID(FWD), FWD, listsUpdatesStore.get(),
            *memoryManager.getBufferManager(), tableSchema->isSingleMultiplicityInDirection(FWD));
    bwdRelTableData =
        std::make_unique<DirectedRelTableData>(tableID, tableSchema->getBoundTableID(BWD),
            tableSchema->getNbrTableID(BWD), BWD, listsUpdatesStore.get(),
            *memoryManager.getBufferManager(), tableSchema->isSingleMultiplicityInDirection(BWD));
    initializeData(tableSchema);
}

void RelTable::initializeData(RelTableSchema* tableSchema) {
    fwdRelTableData->initializeData(tableSchema, wal);
    bwdRelTableData->initializeData(tableSchema, wal);
}

std::vector<AdjLists*> RelTable::getAllAdjLists(table_id_t boundTableID) {
    std::vector<AdjLists*> retVal;
    if (!fwdRelTableData->isSingleMultiplicity() && fwdRelTableData->isBoundTable(boundTableID)) {
        retVal.push_back(fwdRelTableData->getAdjLists());
    }
    if (!bwdRelTableData->isSingleMultiplicity() && bwdRelTableData->isBoundTable(boundTableID)) {
        retVal.push_back(bwdRelTableData->getAdjLists());
    }
    return retVal;
}

std::vector<Column*> RelTable::getAllAdjColumns(table_id_t boundTableID) {
    std::vector<Column*> retVal;
    if (fwdRelTableData->isSingleMultiplicity() && fwdRelTableData->isBoundTable(boundTableID)) {
        retVal.push_back(fwdRelTableData->getAdjColumn());
    }
    if (bwdRelTableData->isSingleMultiplicity() && bwdRelTableData->isBoundTable(boundTableID)) {
        retVal.push_back(bwdRelTableData->getAdjColumn());
    }
    return retVal;
}

void RelTable::prepareCommit() {
    if (listsUpdatesStore->hasUpdates()) {
        wal->addToUpdatedRelTables(tableID);
        prepareCommitForDirection(FWD);
        prepareCommitForDirection(BWD);
    }
}

void RelTable::prepareRollback() {
    if (listsUpdatesStore->hasUpdates()) {
        wal->addToUpdatedRelTables(tableID);
    }
}

void RelTable::checkpointInMemory() {
    performOpOnListsWithUpdates(
        std::bind(&Lists::checkpointInMemoryIfNecessary, std::placeholders::_1),
        std::bind(&RelTable::clearListsUpdatesStore, this));
}

void RelTable::rollback() {
    performOpOnListsWithUpdates(
        std::bind(&Lists::rollbackInMemoryIfNecessary, std::placeholders::_1),
        std::bind(&RelTable::clearListsUpdatesStore, this));
}

// This function assumes that the order of vectors in relPropertyVectorsPerRelTable as:
// [relProp1, relProp2, ..., relPropN] and all vectors are flat.
void RelTable::insertRel(ValueVector* srcNodeIDVector, ValueVector* dstNodeIDVector,
    const std::vector<ValueVector*>& relPropertyVectors) {
    assert(srcNodeIDVector->state->isFlat() && dstNodeIDVector->state->isFlat());
    fwdRelTableData->insertRel(srcNodeIDVector, dstNodeIDVector, relPropertyVectors);
    bwdRelTableData->insertRel(dstNodeIDVector, srcNodeIDVector, relPropertyVectors);
    listsUpdatesStore->insertRelIfNecessary(srcNodeIDVector, dstNodeIDVector, relPropertyVectors);
}

void RelTable::deleteRel(
    ValueVector* srcNodeIDVector, ValueVector* dstNodeIDVector, ValueVector* relIDVector) {
    assert(srcNodeIDVector->state->isFlat() && dstNodeIDVector->state->isFlat() &&
           relIDVector->state->isFlat());
    fwdRelTableData->deleteRel(srcNodeIDVector);
    bwdRelTableData->deleteRel(dstNodeIDVector);
    listsUpdatesStore->deleteRelIfNecessary(srcNodeIDVector, dstNodeIDVector, relIDVector);
}

void RelTable::updateRel(common::ValueVector* srcNodeIDVector, common::ValueVector* dstNodeIDVector,
    common::ValueVector* relIDVector, common::ValueVector* propertyVector, uint32_t propertyID) {
    assert(srcNodeIDVector->state->isFlat() && dstNodeIDVector->state->isFlat() &&
           relIDVector->state->isFlat() && propertyVector->state->isFlat());
    auto srcNode = srcNodeIDVector->getValue<nodeID_t>(
        srcNodeIDVector->state->selVector->selectedPositions[0]);
    auto dstNode = dstNodeIDVector->getValue<nodeID_t>(
        dstNodeIDVector->state->selVector->selectedPositions[0]);
    fwdRelTableData->updateRel(srcNodeIDVector, propertyID, propertyVector);
    bwdRelTableData->updateRel(dstNodeIDVector, propertyID, propertyVector);
    auto relID =
        relIDVector->getValue<relID_t>(relIDVector->state->selVector->selectedPositions[0]);
    ListsUpdateInfo listsUpdateInfo = ListsUpdateInfo{propertyVector, propertyID, relID.offset,
        fwdRelTableData->getListOffset(srcNode, relID.offset),
        bwdRelTableData->getListOffset(dstNode, relID.offset)};
    listsUpdatesStore->updateRelIfNecessary(srcNodeIDVector, dstNodeIDVector, listsUpdateInfo);
}

void RelTable::initEmptyRelsForNewNode(nodeID_t& nodeID) {
    if (fwdRelTableData->isSingleMultiplicity() && fwdRelTableData->isBoundTable(nodeID.tableID)) {
        fwdRelTableData->getAdjColumn()->setNull(nodeID.offset);
    }
    if (bwdRelTableData->isSingleMultiplicity() && bwdRelTableData->isBoundTable(nodeID.tableID)) {
        bwdRelTableData->getAdjColumn()->setNull(nodeID.offset);
    }
    listsUpdatesStore->initNewlyAddedNodes(nodeID);
}

void RelTable::batchInitEmptyRelsForNewNodes(
    const RelTableSchema* relTableSchema, uint64_t numNodesInTable) {
    fwdRelTableData->batchInitEmptyRelsForNewNodes(
        relTableSchema, numNodesInTable, wal->getDirectory());
    bwdRelTableData->batchInitEmptyRelsForNewNodes(
        relTableSchema, numNodesInTable, wal->getDirectory());
}

void RelTable::addProperty(Property property, RelTableSchema& relTableSchema) {
    fwdRelTableData->addProperty(property, wal);
    bwdRelTableData->addProperty(property, wal);
    listsUpdatesStore->updateSchema(relTableSchema);
}

void RelTable::updateListOP(
    ListsUpdateIterator* listsUpdateIterator, offset_t nodeOffset, InMemList& inMemList) {
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

std::unique_ptr<ListsUpdateIteratorsForDirection> RelTable::getListsUpdateIteratorsForDirection(
    RelDataDirection relDirection) const {
    return relDirection == FWD ? fwdRelTableData->getListsUpdateIteratorsForDirection() :
                                 bwdRelTableData->getListsUpdateIteratorsForDirection();
}

void DirectedRelTableData::removeProperty(property_id_t propertyID) {
    for (auto& [propertyID_, propertyColumn] : propertyColumns) {
        if (propertyID_ == propertyID) {
            propertyColumns.erase(propertyID_);
            break;
        }
    }

    for (auto& [propertyID_, propertyList] : propertyLists) {
        if (propertyID_ == propertyID) {
            propertyLists.erase(propertyID_);
            break;
        }
    }
}

void DirectedRelTableData::addProperty(Property& property, WAL* wal) {
    if (isSingleMultiplicity()) {
        propertyColumns.emplace(property.propertyID,
            ColumnFactory::getColumn(
                StorageUtils::getRelPropertyColumnStructureIDAndFName(
                    wal->getDirectory(), tableID, direction, property.propertyID),
                property.dataType, &bufferManager, wal));
    } else {
        propertyLists.emplace(property.propertyID,
            ListsFactory::getLists(StorageUtils::getRelPropertyListsStructureIDAndFName(
                                       wal->getDirectory(), tableID, direction, property),
                property.dataType, adjLists->getHeaders(), &bufferManager, wal, listsUpdatesStore));
    }
}

void DirectedRelTableData::batchInitEmptyRelsForNewNodes(
    const RelTableSchema* relTableSchema, uint64_t numNodesInTable, const std::string& directory) {
    if (!isSingleMultiplicity()) {
        StorageUtils::initializeListsHeaders(relTableSchema, numNodesInTable, directory, direction);
    }
}

void RelTable::prepareCommitForDirection(RelDataDirection relDirection) {
    auto& listsUpdatesPerChunk = listsUpdatesStore->getListsUpdatesPerChunk(relDirection);
    if (isSingleMultiplicityInDirection(relDirection) || listsUpdatesPerChunk.empty()) {
        return;
    }
    auto listsUpdateIteratorsForDirection = getListsUpdateIteratorsForDirection(relDirection);
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
            auto adjLists = getAdjLists(relDirection);
            if (listsUpdatesForNodeOffset->isNewlyAddedNode) {
                prepareCommitForListWithUpdateStoreDataOnly(adjLists, nodeOffset,
                    listsUpdatesForNodeOffset.get(), relDirection,
                    listsUpdateIteratorsForDirection.get(), updateListOP);
            } else {
                prepareCommitForList(adjLists, nodeOffset, listsUpdatesForNodeOffset.get(),
                    relDirection, listsUpdateIteratorsForDirection.get());
            }
        }
    }
    listsUpdateIteratorsForDirection->doneUpdating();
}

void RelTable::prepareCommitForListWithUpdateStoreDataOnly(AdjLists* adjLists, offset_t nodeOffset,
    ListsUpdatesForNodeOffset* listsUpdatesForNodeOffset, RelDataDirection relDirection,
    ListsUpdateIteratorsForDirection* listsUpdateIteratorsForDirection,
    const std::function<void(ListsUpdateIterator* listsUpdateIterator, offset_t,
        InMemList& inMemList)>& opOnListsUpdateIterators) {
    auto inMemAdjLists = adjLists->createInMemListWithDataFromUpdateStoreOnly(
        nodeOffset, listsUpdatesForNodeOffset->insertedRelsTupleIdxInFT);
    opOnListsUpdateIterators(listsUpdateIteratorsForDirection->getListUpdateIteratorForAdjList(),
        nodeOffset, *inMemAdjLists);
    for (auto& [propertyID, listUpdateIteratorForPropertyList] :
        listsUpdateIteratorsForDirection->getListsUpdateIteratorsForPropertyLists()) {
        auto inMemPropLists = getPropertyLists(relDirection, propertyID)
                                  ->createInMemListWithDataFromUpdateStoreOnly(nodeOffset,
                                      listsUpdatesForNodeOffset->insertedRelsTupleIdxInFT);
        opOnListsUpdateIterators(
            listUpdateIteratorForPropertyList.get(), nodeOffset, *inMemPropLists);
    }
}

void RelTable::prepareCommitForList(AdjLists* adjLists, offset_t nodeOffset,
    ListsUpdatesForNodeOffset* listsUpdatesForNodeOffset, RelDataDirection relDirection,
    ListsUpdateIteratorsForDirection* listsUpdateIteratorsForDirection) {
    auto relIDLists =
        (RelIDList*)getPropertyLists(relDirection, RelTableSchema::INTERNAL_REL_ID_PROPERTY_ID);
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
            getPropertyLists(relDirection, propertyID)
                ->writeToInMemList(nodeOffset, listsUpdatesForNodeOffset->insertedRelsTupleIdxInFT,
                    deletedRelOffsets,
                    &listsUpdatesForNodeOffset->updatedPersistentListOffsets.at(propertyID));
        listUpdateIteratorForPropertyList->updateList(nodeOffset, *inMemPropLists);
    }
}

} // namespace storage
} // namespace kuzu
