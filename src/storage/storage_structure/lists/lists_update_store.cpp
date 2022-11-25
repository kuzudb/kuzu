#include "storage/storage_structure/lists/lists_update_store.h"

#include "storage/storage_structure/lists/lists.h"

namespace kuzu {
namespace storage {

ListsUpdateStore::ListsUpdateStore(MemoryManager& memoryManager, RelTableSchema& relTableSchema)
    : relTableSchema{relTableSchema} {
    auto factorizedTableSchema = make_unique<FactorizedTableSchema>();
    // The first two columns of factorizedTable are for srcNodeID and dstNodeID.
    factorizedTableSchema->appendColumn(
        make_unique<ColumnSchema>(false /* isUnflat */, 0 /* dataChunkPos */, sizeof(nodeID_t)));
    factorizedTableSchema->appendColumn(
        make_unique<ColumnSchema>(false /* isUnflat */, 0 /* dataChunkPos */, sizeof(nodeID_t)));
    for (auto& relProperty : relTableSchema.properties) {
        propertyIDToColIdxMap.emplace(
            relProperty.propertyID, factorizedTableSchema->getNumColumns());
        factorizedTableSchema->appendColumn(make_unique<ColumnSchema>(false /* isUnflat */,
            0 /* dataChunkPos */, Types::getDataTypeSize(relProperty.dataType)));
    }
    nodeDataChunk = make_shared<DataChunk>(2);
    nodeDataChunk->state->currIdx = 0;
    srcNodeVector = make_shared<ValueVector>(NODE_ID, &memoryManager);
    nodeDataChunk->insert(0 /* pos */, srcNodeVector);
    dstNodeVector = make_shared<ValueVector>(NODE_ID, &memoryManager);
    nodeDataChunk->insert(1 /* pos */, dstNodeVector);
    factorizedTable =
        make_unique<FactorizedTable>(&memoryManager, std::move(factorizedTableSchema));
    initListUpdatesPerTablePerDirection();
}

bool ListsUpdateStore::isListEmptyInPersistentStore(
    ListFileID& listFileID, node_offset_t nodeOffset) const {
    auto relNodeTableAndDir = getRelNodeTableAndDirFromListFileID(listFileID);
    auto& listUpdatesPerChunk = listUpdatesPerTablePerDirection[relNodeTableAndDir.dir].at(
        relNodeTableAndDir.srcNodeTableID);
    auto chunkIdx = StorageUtils::getListChunkIdx(nodeOffset);
    if (!listUpdatesPerChunk.contains(chunkIdx) ||
        !listUpdatesPerChunk.at(chunkIdx).contains(nodeOffset)) {
        return false;
    }
    return listUpdatesPerChunk.at(chunkIdx).at(nodeOffset).emptyListInPersistentStore;
}

bool ListsUpdateStore::hasUpdates() const {
    for (auto relDirection : REL_DIRECTIONS) {
        for (const auto& listUpdatesPerTable : listUpdatesPerTablePerDirection[relDirection]) {
            if (!listUpdatesPerTable.second.empty()) {
                return true;
            }
        }
    }
    return false;
}

// Note: This function also resets the overflowptr of each string in inMemList if necessary.
void ListsUpdateStore::readInsertionsToList(ListFileID& listFileID, vector<uint64_t> tupleIdxes,
    InMemList& inMemList, uint64_t numElementsInPersistentStore, DiskOverflowFile* diskOverflowFile,
    DataType dataType, NodeIDCompressionScheme* nodeIDCompressionScheme) {
    factorizedTable->copyToInMemList(getColIdxInFT(listFileID), tupleIdxes, inMemList.getListData(),
        inMemList.nullMask.get(), numElementsInPersistentStore, diskOverflowFile, dataType,
        nodeIDCompressionScheme);
}

void ListsUpdateStore::insertRelIfNecessary(shared_ptr<ValueVector>& srcNodeIDVector,
    shared_ptr<ValueVector>& dstNodeIDVector, vector<shared_ptr<ValueVector>>& relPropertyVectors) {
    auto srcNodeID =
        srcNodeIDVector->getValue<nodeID_t>(srcNodeIDVector->state->getPositionOfCurrIdx());
    auto dstNodeID =
        dstNodeIDVector->getValue<nodeID_t>(dstNodeIDVector->state->getPositionOfCurrIdx());
    bool hasInsertedToFT = false;
    auto vectorsToAppendToFT = vector<shared_ptr<ValueVector>>{srcNodeIDVector, dstNodeIDVector};
    vectorsToAppendToFT.insert(
        vectorsToAppendToFT.end(), relPropertyVectors.begin(), relPropertyVectors.end());
    for (auto direction : REL_DIRECTIONS) {
        auto boundNodeID = direction == RelDirection::FWD ? srcNodeID : dstNodeID;
        auto chunkIdx = StorageUtils::getListChunkIdx(boundNodeID.offset);
        if (listUpdatesPerTablePerDirection[direction].contains(boundNodeID.tableID)) {
            if (!hasInsertedToFT) {
                factorizedTable->append(vectorsToAppendToFT);
                hasInsertedToFT = true;
            }
            listUpdatesPerTablePerDirection[direction]
                .at(boundNodeID.tableID)[chunkIdx][boundNodeID.offset]
                .insertedRelsTupleIdxInFT.push_back(factorizedTable->getNumTuples() - 1);
        }
    }
}

uint64_t ListsUpdateStore::getNumInsertedRelsForNodeOffset(
    ListFileID& listFileID, node_offset_t nodeOffset) const {
    auto chunkIdx = StorageUtils::getListChunkIdx(nodeOffset);
    auto relNodeTableAndDir = getRelNodeTableAndDirFromListFileID(listFileID);
    auto listUpdatesPerTable = listUpdatesPerTablePerDirection[relNodeTableAndDir.dir].at(
        relNodeTableAndDir.srcNodeTableID);
    if (!listUpdatesPerTable.contains(chunkIdx) ||
        !listUpdatesPerTable[chunkIdx].contains(nodeOffset)) {
        return 0;
    }
    return listUpdatesPerTable.at(chunkIdx).at(nodeOffset).insertedRelsTupleIdxInFT.size();
}

void ListsUpdateStore::readValues(ListFileID& listFileID, ListSyncState& listSyncState,
    shared_ptr<ValueVector> valueVector) const {
    auto numTuplesToRead = listSyncState.getNumValuesToRead();
    auto nodeOffset = listSyncState.getBoundNodeOffset();
    if (numTuplesToRead == 0) {
        valueVector->state->initOriginalAndSelectedSize(0);
        return;
    }
    auto vectorsToRead = vector<shared_ptr<ValueVector>>{valueVector};
    auto columnsToRead = vector<uint32_t>{getColIdxInFT(listFileID)};
    auto relNodeTableAndDir = getRelNodeTableAndDirFromListFileID(listFileID);
    auto listUpdates = listUpdatesPerTablePerDirection[relNodeTableAndDir.dir]
                           .at(relNodeTableAndDir.srcNodeTableID)
                           .at(StorageUtils::getListChunkIdx(nodeOffset))
                           .at(nodeOffset);
    factorizedTable->lookup(vectorsToRead, columnsToRead, listUpdates.insertedRelsTupleIdxInFT,
        listSyncState.getStartElemOffset(), numTuplesToRead);
    valueVector->state->originalSize = numTuplesToRead;
}

uint32_t ListsUpdateStore::getColIdxInFT(ListFileID& listFileID) const {
    if (listFileID.listType == ADJ_LISTS) {
        return listFileID.adjListsID.relNodeTableAndDir.dir == FWD ? 1 : 0;
    } else {
        return propertyIDToColIdxMap.at(listFileID.relPropertyListID.propertyID);
    }
}

void ListsUpdateStore::initListUpdatesPerTablePerDirection() {
    listUpdatesPerTablePerDirection.clear();
    for (auto direction : REL_DIRECTIONS) {
        listUpdatesPerTablePerDirection.push_back(map<table_id_t, ListUpdatesPerChunk>{});
        auto boundTableIDs = direction == RelDirection::FWD ?
                                 relTableSchema.getUniqueSrcTableIDs() :
                                 relTableSchema.getUniqueDstTableIDs();
        if (relTableSchema.isStoredAsLists(direction)) {
            for (auto boundTableID : boundTableIDs) {
                listUpdatesPerTablePerDirection[direction].emplace(
                    boundTableID, ListUpdatesPerChunk{});
            }
        }
    }
}

} // namespace storage
} // namespace kuzu
