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
    initEmptyListInPersistentStoreAndInsertedRels();
}

bool AdjAndPropertyListsUpdateStore::isEmptyListInPersistentStoreForNodeOffset(
    ListFileID& listFileID, node_offset_t nodeOffset) const {
    auto relNodeTableAndDir = getRelNodeTableAndDirFromListFileID(listFileID);
    auto& emptyListInPersistentStoreAndInsertedRelsForTable =
        emptyListInPersistentStoreAndinsertedRelsPerTableIDPerDirection[relNodeTableAndDir.dir].at(
            relNodeTableAndDir.srcNodeTableID);
    auto chunkIdx = StorageUtils::getListChunkIdx(nodeOffset);
    if (!emptyListInPersistentStoreAndInsertedRelsForTable.contains(chunkIdx) ||
        !emptyListInPersistentStoreAndInsertedRelsForTable.at(chunkIdx).contains(nodeOffset)) {
        return false;
    }
    return emptyListInPersistentStoreAndInsertedRelsForTable
        .at(StorageUtils::getListChunkIdx(nodeOffset))
        .at(nodeOffset)
        .emptyListInPersistentStore;
}

bool AdjAndPropertyListsUpdateStore::hasEmptyListInPersistentStoreOrInsertedRels() const {
    for (auto relDirection : REL_DIRECTIONS) {
        for (auto emptyListInPersistentStoreAndInsertedRelsPerTable :
            emptyListInPersistentStoreAndinsertedRelsPerTableIDPerDirection[relDirection]) {
            if (!emptyListInPersistentStoreAndInsertedRelsPerTable.second.empty()) {
                return true;
            }
        }
    }
    return false;
}

void AdjAndPropertyListsUpdateStore::readToListAndUpdateOverflowIfNecessary(ListFileID& listFileID,
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
        if (emptyListInPersistentStoreAndinsertedRelsPerTableIDPerDirection[direction].contains(
                nodeID.tableID)) {
            if (!hasInsertedToFT) {
                factorizedTable->append(srcDstNodeIDAndRelProperties);
                hasInsertedToFT = true;
            }
            emptyListInPersistentStoreAndinsertedRelsPerTableIDPerDirection[direction]
                .at(nodeID.tableID)[chunkIdx][nodeID.offset]
                .insertedRelsTupleIdxInFT.push_back(factorizedTable->getNumTuples() - 1);
        }
    }
}

uint64_t AdjAndPropertyListsUpdateStore::getNumInsertedRelsForNodeOffset(
    ListFileID& listFileID, node_offset_t nodeOffset) const {
    auto chunkIdx = StorageUtils::getListChunkIdx(nodeOffset);
    auto relNodeTableAndDir = getRelNodeTableAndDirFromListFileID(listFileID);
    auto emptyListInPersistentStoreAndinsertedRelsPerChunk =
        emptyListInPersistentStoreAndinsertedRelsPerTableIDPerDirection[relNodeTableAndDir.dir].at(
            relNodeTableAndDir.srcNodeTableID);
    if (!emptyListInPersistentStoreAndinsertedRelsPerChunk.contains(chunkIdx) ||
        !emptyListInPersistentStoreAndinsertedRelsPerChunk[chunkIdx].contains(nodeOffset)) {
        return 0;
    }
    return emptyListInPersistentStoreAndinsertedRelsPerChunk.at(chunkIdx)
        .at(nodeOffset)
        .insertedRelsTupleIdxInFT.size();
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
    auto tupleIdxesToRead =
        emptyListInPersistentStoreAndinsertedRelsPerTableIDPerDirection[relNodeTableAndDir.dir]
            .at(relNodeTableAndDir.srcNodeTableID)
            .at(StorageUtils::getListChunkIdx(nodeOffset))
            .at(nodeOffset)
            .insertedRelsTupleIdxInFT;
    factorizedTable->lookup(vectorsToRead, columnsToRead, tupleIdxesToRead,
        listSyncState.getStartElemOffset(), numTuplesToRead);
    valueVector->state->originalSize = numTuplesToRead;
}

void AdjAndPropertyListsUpdateStore::deleteRels(nodeID_t& nodeID) {
    for (auto relDirection : REL_DIRECTIONS) {
        if (relTableSchema.isRelPropertyList(relDirection)) {
            emptyListInPersistentStoreAndinsertedRelsPerTableIDPerDirection[relDirection]
                .at(nodeID.tableID)[StorageUtils::getListChunkIdx(nodeID.offset)][nodeID.offset]
                .deleteRels();
        }
    }
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
    if (relTableSchema.isRelPropertyList(RelDirection::FWD) &&
        !emptyListInPersistentStoreAndinsertedRelsPerTableIDPerDirection[RelDirection::FWD]
             .contains(srcNodeID.tableID)) {
        getErrorMsgForInvalidTableID(
            srcNodeID.tableID, true /* isSrcTableID */, relTableSchema.tableName);
    } else if (relTableSchema.isRelPropertyList(RelDirection::BWD) &&
               !emptyListInPersistentStoreAndinsertedRelsPerTableIDPerDirection[RelDirection::BWD]
                    .contains(dstNodeID.tableID)) {
        getErrorMsgForInvalidTableID(
            dstNodeID.tableID, false /* isSrcTableID */, relTableSchema.tableName);
    }
}

void AdjAndPropertyListsUpdateStore::initEmptyListInPersistentStoreAndInsertedRels() {
    emptyListInPersistentStoreAndinsertedRelsPerTableIDPerDirection.clear();
    auto srcDstTableIDs = relTableSchema.getSrcDstTableIDs();
    for (auto direction : REL_DIRECTIONS) {
        emptyListInPersistentStoreAndinsertedRelsPerTableIDPerDirection.push_back(
            map<table_id_t, EmptyListInPersistentStoreAndInsertedRelsPerChunk>{});
        auto tableIDs = direction == RelDirection::FWD ? srcDstTableIDs.srcTableIDs :
                                                         srcDstTableIDs.dstTableIDs;
        if (relTableSchema.isRelPropertyList(direction)) {
            for (auto tableID : tableIDs) {
                emptyListInPersistentStoreAndinsertedRelsPerTableIDPerDirection[direction].emplace(
                    tableID, EmptyListInPersistentStoreAndInsertedRelsPerChunk{});
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
