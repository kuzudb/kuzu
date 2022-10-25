#include "src/storage/storage_structure/include/lists/adj_and_property_lists_update_store.h"

#include "src/storage/storage_structure/include/lists/lists.h"

namespace graphflow {
namespace storage {

AdjAndPropertyListsUpdateStore::AdjAndPropertyListsUpdateStore(
    MemoryManager& memoryManager, RelTableSchema& relTableSchema)
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
    factorizedTable = make_unique<FactorizedTable>(&memoryManager, move(factorizedTableSchema));
    initListUpdatesPerTablePerDirection();
}

bool AdjAndPropertyListsUpdateStore::isListEmptyInPersistentStore(
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

bool AdjAndPropertyListsUpdateStore::hasUpdates() const {
    for (auto relDirection : REL_DIRECTIONS) {
        for (auto listUpdatesPerTable : listUpdatesPerTablePerDirection[relDirection]) {
            if (!listUpdatesPerTable.second.empty()) {
                return true;
            }
        }
    }
    return false;
}

// Note: This function also resets the overflowptr of each string in inMemList if necessary.
void AdjAndPropertyListsUpdateStore::readInsertionsToList(ListFileID& listFileID,
    vector<uint64_t> tupleIdxes, InMemList& inMemList, uint64_t numElementsInPersistentStore,
    DiskOverflowFile* diskOverflowFile, DataType dataType,
    NodeIDCompressionScheme* nodeIDCompressionScheme) {
    factorizedTable->copyToInMemList(getColIdxInFT(listFileID), tupleIdxes, inMemList.getListData(),
        inMemList.nullMask.get(), numElementsInPersistentStore, diskOverflowFile, dataType,
        nodeIDCompressionScheme);
}

void AdjAndPropertyListsUpdateStore::insertRelIfNecessary(
    vector<shared_ptr<ValueVector>>& srcDstNodeIDAndRelProperties) {
    validateSrcDstNodeIDAndRelProperties(srcDstNodeIDAndRelProperties);
    auto srcNodeVector = srcDstNodeIDAndRelProperties[0];
    auto dstNodeVector = srcDstNodeIDAndRelProperties[1];
    auto pos = srcNodeVector->state->selVector
                   ->selectedPositions[srcNodeVector->state->getPositionOfCurrIdx()];
    auto srcNodeID = ((nodeID_t*)srcNodeVector->values)[pos];
    auto dstNodeID = ((nodeID_t*)dstNodeVector->values)[pos];
    bool hasInsertedToFT = false;
    for (auto direction : REL_DIRECTIONS) {
        auto nodeID = direction == RelDirection::FWD ? srcNodeID : dstNodeID;
        auto chunkIdx = StorageUtils::getListChunkIdx(nodeID.offset);
        if (listUpdatesPerTablePerDirection[direction].contains(nodeID.tableID)) {
            if (!hasInsertedToFT) {
                factorizedTable->append(srcDstNodeIDAndRelProperties);
                hasInsertedToFT = true;
            }
            listUpdatesPerTablePerDirection[direction]
                .at(nodeID.tableID)[chunkIdx][nodeID.offset]
                .insertedRelsTupleIdxInFT.push_back(factorizedTable->getNumTuples() - 1);
        }
    }
}

uint64_t AdjAndPropertyListsUpdateStore::getNumInsertedRelsForNodeOffset(
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

void AdjAndPropertyListsUpdateStore::readValues(ListFileID& listFileID,
    ListSyncState& listSyncState, shared_ptr<ValueVector> valueVector) const {
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

uint32_t AdjAndPropertyListsUpdateStore::getColIdxInFT(ListFileID& listFileID) const {
    if (listFileID.listType == ADJ_LISTS) {
        return listFileID.adjListsID.relNodeTableAndDir.dir == FWD ? 1 : 0;
    } else {
        return propertyIDToColIdxMap.at(listFileID.relPropertyListID.propertyID);
    }
}

// TODO(Ziyi): This function is designed to help implementing the front-end of insertRels. Once the
// front-end of insertRels has been implemented, we should delete this function.
void AdjAndPropertyListsUpdateStore::validateSrcDstNodeIDAndRelProperties(
    vector<shared_ptr<ValueVector>> srcDstNodeIDAndRelProperties) const {
    // Checks whether the number of vectors inside srcDstNodeIDAndRelProperties matches the number
    // of columns in factorizedTable.
    if (factorizedTable->getTableSchema()->getNumColumns() != srcDstNodeIDAndRelProperties.size()) {
        throw InternalException(
            StringUtils::string_format("Expected number of valueVectors: %d. Given: %d.",
                relTableSchema.properties.size() + 2, srcDstNodeIDAndRelProperties.size()));
    }
    // Checks whether the dataType of each given vector matches the one defined in tableSchema.
    for (auto i = 0u; i < srcDstNodeIDAndRelProperties.size(); i++) {
        // Note that: we store srcNodeID and dstNodeID in the first two columns of factorizedTable.
        // So, we only need to compare the columns whose colIdx > 2 with tableSchema.
        if (i >= 2 && srcDstNodeIDAndRelProperties[i]->dataType !=
                          relTableSchema.properties[i - 2].dataType) {
            throw InternalException(StringUtils::string_format(
                "Expected vector with type %s, Given: %s.",
                Types::dataTypeToString(relTableSchema.properties[i - 2].dataType.typeID).c_str(),
                Types::dataTypeToString(srcDstNodeIDAndRelProperties[i]->dataType.typeID).c_str()));
        } else if (i < 2 &&
                   srcDstNodeIDAndRelProperties[i]->dataType.typeID != DataTypeID(NODE_ID)) {
            throw InternalException("The first two vectors of srcDstNodeIDAndRelProperties should "
                                    "be src/dstNodeVector.");
        }
    }
    // Checks whether the srcTableID and dstTableID is a valid src/dst table ID defined in
    // tableSchema.
    auto srcNodeVector = srcDstNodeIDAndRelProperties[0];
    auto dstNodeVector = srcDstNodeIDAndRelProperties[1];
    auto pos = srcNodeVector->state->selVector
                   ->selectedPositions[srcNodeVector->state->getPositionOfCurrIdx()];
    auto srcNodeID = ((nodeID_t*)srcNodeVector->values)[pos];
    auto dstNodeID = ((nodeID_t*)dstNodeVector->values)[pos];
    if (relTableSchema.isStoredAsLists(RelDirection::FWD) &&
        !listUpdatesPerTablePerDirection[RelDirection::FWD].contains(srcNodeID.tableID)) {
        getErrorMsgForInvalidTableID(
            srcNodeID.tableID, true /* isSrcTableID */, relTableSchema.tableName);
    } else if (relTableSchema.isStoredAsLists(RelDirection::BWD) &&
               !listUpdatesPerTablePerDirection[RelDirection::BWD].contains(dstNodeID.tableID)) {
        getErrorMsgForInvalidTableID(
            dstNodeID.tableID, false /* isSrcTableID */, relTableSchema.tableName);
    }
}

void AdjAndPropertyListsUpdateStore::initListUpdatesPerTablePerDirection() {
    listUpdatesPerTablePerDirection.clear();
    auto srcDstTableIDs = relTableSchema.getSrcDstTableIDs();
    for (auto direction : REL_DIRECTIONS) {
        listUpdatesPerTablePerDirection.push_back(map<table_id_t, ListUpdatesPerChunk>{});
        auto tableIDs = direction == RelDirection::FWD ? srcDstTableIDs.srcTableIDs :
                                                         srcDstTableIDs.dstTableIDs;
        if (relTableSchema.isStoredAsLists(direction)) {
            for (auto tableID : tableIDs) {
                listUpdatesPerTablePerDirection[direction].emplace(tableID, ListUpdatesPerChunk{});
            }
        }
    }
}

void AdjAndPropertyListsUpdateStore::getErrorMsgForInvalidTableID(
    uint64_t tableID, bool isSrcTableID, string tableName) {
    throw InternalException(
        StringUtils::string_format("TableID: %d is not a valid %s tableID in rel %s.", tableID,
            isSrcTableID ? "src" : "dst", tableName.c_str()));
}

} // namespace storage
} // namespace graphflow
