#include "src/storage/storage_structure/include/lists/rel_update_store.h"

namespace graphflow {
namespace storage {

ListUpdateStore::ListUpdateStore(MemoryManager& memoryManager, RelTableSchema& relTableSchema) {
    auto factorizedTableSchema = make_unique<FactorizedTableSchema>();
    // FactorizedTable column format:
    // [srcNodeID, dstNodeID, prop1, prop2..., propN].
    factorizedTableSchema->appendColumn(
        make_unique<ColumnSchema>(false /* isUnflat */, 0, sizeof(nodeID_t)));
    factorizedTableSchema->appendColumn(
        make_unique<ColumnSchema>(false /* isUnflat */, 0, sizeof(nodeID_t)));
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
}

void ListUpdateStore::addRel(vector<shared_ptr<ValueVector>>& srcDstNodeIDAndRelProperties) {
    auto srcNodeVector = srcDstNodeIDAndRelProperties[0];
    auto srcNodeOffset =
        srcNodeVector->readNodeOffset(srcNodeVector->state->getPositionOfCurrIdx());
    auto chunkIdx = StorageUtils::getListChunkIdx(srcNodeOffset);
    if (!insertedEdgeTupleIdxs.contains(chunkIdx)) {
        insertedEdgeTupleIdxs.emplace(chunkIdx, InsertedEdgeTupleIdxs{});
    }
    factorizedTable->append(srcDstNodeIDAndRelProperties);
    insertedEdgeTupleIdxs[chunkIdx][srcNodeOffset].push_back(factorizedTable->getNumTuples() - 1);
}

uint64_t ListUpdateStore::getNumInsertedRelsForNodeOffset(node_offset_t nodeOffset) const {
    auto chunkIdx = StorageUtils::getListChunkIdx(nodeOffset);
    if (!insertedEdgeTupleIdxs.contains(chunkIdx)) {
        return 0;
    }
    auto& chunks = insertedEdgeTupleIdxs.at(chunkIdx);
    return !chunks.contains(nodeOffset) ? 0 : chunks.at(nodeOffset).size();
}

void ListUpdateStore::readValues(
    ListSyncState& listSyncState, shared_ptr<ValueVector> valueVector, uint32_t colIdx) const {
    auto numTuplesToRead = listSyncState.getNumValuesToRead();
    auto nodeOffset = listSyncState.getBoundNodeOffset();
    if (numTuplesToRead == 0) {
        valueVector->state->initOriginalAndSelectedSize(0);
        return;
    }
    auto vectorsToRead = vector<shared_ptr<ValueVector>>{valueVector};
    auto columnsToRead = vector<uint32_t>{colIdx};
    auto tupleIdxesToRead =
        insertedEdgeTupleIdxs.at(StorageUtils::getListChunkIdx(nodeOffset)).at(nodeOffset);
    factorizedTable->lookup(vectorsToRead, columnsToRead, tupleIdxesToRead,
        listSyncState.getStartElemOffset(), numTuplesToRead);
    valueVector->state->originalSize = numTuplesToRead;
}

void RelUpdateStore::addRel(vector<shared_ptr<ValueVector>> srcDstNodeIDAndRelProperties) {
    validateSrcDstNodeIDAndRelProperties(srcDstNodeIDAndRelProperties);
    // This function assumes that the last two vectors of srcDstNodeIDAndRelProperties are
    // srcNodeVector and dstNodeVector.
    auto srcNodeVector = srcDstNodeIDAndRelProperties[0];
    auto dstNodeVector = srcDstNodeIDAndRelProperties[1];
    auto srcNodeID =
        ((nodeID_t*)srcNodeVector
                ->values)[srcNodeVector->state->selVector
                              ->selectedPositions[srcNodeVector->state->getPositionOfCurrIdx()]];
    auto dstNodeID =
        ((nodeID_t*)dstNodeVector
                ->values)[dstNodeVector->state->selVector
                              ->selectedPositions[dstNodeVector->state->getPositionOfCurrIdx()]];
    for (auto direction : REL_DIRECTIONS) {
        auto tableID = direction == FWD ? srcNodeID.tableID : dstNodeID.tableID;
        if (!listUpdateStoresPerDirection[direction].contains(tableID)) {
            throw InternalException(StringUtils::string_format(
                "ListUpdateStore for tableID: %d doesn't exist.", tableID));
        }
        listUpdateStoresPerDirection[direction].at(tableID)->addRel(srcDstNodeIDAndRelProperties);
        swap(srcDstNodeIDAndRelProperties[0], srcDstNodeIDAndRelProperties[1]);
    }
}

shared_ptr<ListUpdateStore> RelUpdateStore::getOrCreateListUpdateStore(
    RelDirection relDirection, table_id_t tableID) {
    if (!listUpdateStoresPerDirection[relDirection].contains(tableID)) {
        listUpdateStoresPerDirection[relDirection].emplace(
            tableID, make_unique<ListUpdateStore>(memoryManager, relTableSchema));
    }
    return listUpdateStoresPerDirection[relDirection].at(tableID);
}

void RelUpdateStore::validateSrcDstNodeIDAndRelProperties(
    vector<shared_ptr<ValueVector>> srcDstNodeIDAndRelProperties) const {
    if (relTableSchema.properties.size() + 2 != srcDstNodeIDAndRelProperties.size()) {
        throw InternalException(
            StringUtils::string_format("Expected number of valueVectors: %d. Given: %d.",
                relTableSchema.properties.size() + 2, srcDstNodeIDAndRelProperties.size()));
    }
    for (auto i = 0u; i < srcDstNodeIDAndRelProperties.size(); i++) {
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
}

} // namespace storage
} // namespace graphflow
