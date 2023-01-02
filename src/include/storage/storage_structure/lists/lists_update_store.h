#pragma once

#include <map>

#include "catalog/catalog_structs.h"
#include "common/data_chunk/data_chunk.h"
#include "common/types/node_id_t.h"
#include "common/types/types.h"
#include "processor/result/factorized_table.h"
#include "storage/storage_structure/lists/list_handle.h"
#include "storage/storage_utils.h"

namespace kuzu {
namespace storage {

using namespace processor;
using namespace catalog;

struct ListUpdates {
public:
    ListUpdates() : isNewlyAddedNode{false} {}

    inline bool hasUpdates() const {
        return isNewlyAddedNode || !insertedRelsTupleIdxInFT.empty() || !deletedRelIDs.empty();
    }

public:
    bool isNewlyAddedNode;
    vector<uint64_t> insertedRelsTupleIdxInFT;
    unordered_set<int64_t> deletedRelIDs;
};

using ListUpdatesPerNode = map<node_offset_t, ListUpdates>;
using ListUpdatesPerChunk = map<uint64_t, ListUpdatesPerNode>;

struct InMemList;

class ListsUpdateStore {

public:
    ListsUpdateStore(MemoryManager& memoryManager, RelTableSchema& relTableSchema);

    inline bool isEmpty() const { return factorizedTable->isEmpty(); }
    inline void clear() {
        factorizedTable->clear();
        initListUpdatesPerTablePerDirection();
    }
    inline map<table_id_t, ListUpdatesPerChunk>& getListUpdatesPerBoundNodeTableOfDirection(
        RelDirection relDirection) {
        return listUpdatesPerTablePerDirection[relDirection];
    }

    uint64_t getNumDeletedRels(ListFileID& listFileID, node_offset_t nodeOffset) const;

    bool isNewlyAddedNode(ListFileID& listFileID, node_offset_t nodeOffset) const;

    bool isRelDeletedInPersistentStore(
        ListFileID& listFileID, node_offset_t nodeOffset, int64_t relID) const;

    bool hasUpdates() const;

    void readInsertionsToList(ListFileID& listFileID, vector<uint64_t> tupleIdxes,
        InMemList& inMemList, uint64_t numElementsInPersistentStore,
        DiskOverflowFile* diskOverflowFile, DataType dataType,
        NodeIDCompressionScheme* nodeIDCompressionScheme);

    // If this is a one-to-one relTable, all properties are stored in columns.
    // In this case, the listsUpdateStore should not store the insert rels in FT.
    void insertRelIfNecessary(const shared_ptr<ValueVector>& srcNodeIDVector,
        const shared_ptr<ValueVector>& dstNodeIDVector,
        const vector<shared_ptr<ValueVector>>& relPropertyVectors);

    void deleteRelIfNecessary(const shared_ptr<ValueVector>& srcNodeIDVector,
        const shared_ptr<ValueVector>& dstNodeIDVector, const shared_ptr<ValueVector>& relIDVector);

    uint64_t getNumInsertedRelsForNodeOffset(
        ListFileID& listFileID, node_offset_t nodeOffset) const;

    void readValues(
        ListFileID& listFileID, ListHandle& listHandle, shared_ptr<ValueVector> valueVector) const;

    void initNewlyAddedNodes(nodeID_t& nodeID) {
        for (auto direction : REL_DIRECTIONS) {
            if (listUpdatesPerTablePerDirection[direction].contains(nodeID.tableID)) {
                listUpdatesPerTablePerDirection[direction][nodeID.tableID]
                                               [StorageUtils::getListChunkIdx(nodeID.offset)]
                                               [nodeID.offset]
                                                   .isNewlyAddedNode = true;
            }
        }
    }

    bool hasAnyDeletedRelsInPersistentStore(ListFileID& listFileID, node_offset_t nodeOffset) const;

private:
    static inline RelNodeTableAndDir getRelNodeTableAndDirFromListFileID(ListFileID& listFileID) {
        return listFileID.listType == ListType::ADJ_LISTS ?
                   listFileID.adjListsID.relNodeTableAndDir :
                   listFileID.relPropertyListID.relNodeTableAndDir;
    }
    inline int64_t getTupleIdxIfInsertedRel(int64_t relID) {
        return factorizedTable->findValueInFlatColumn(INTERNAL_REL_ID_IDX_IN_FT, relID);
    }

    uint32_t getColIdxInFT(ListFileID& listFileID) const;

    void initListUpdatesPerTablePerDirection();

private:
    /* ListsUpdateStore stores all inserted edges in a factorizedTable in the format:
     * [srcNodeID, dstNodeID, relID, relProp1, relProp2, ..., relPropN]. In order to efficiently
     * find the insertedEdges for a nodeTable, we introduce a mapping which stores the tupleIdxes of
     * insertedEdges for each nodeTable per direction.
     */
    static constexpr uint64_t SRC_TABLE_ID_IDX_IN_FT = 0;
    static constexpr uint64_t DST_TABLE_ID_IDX_IN_FT = 1;
    static constexpr uint64_t INTERNAL_REL_ID_IDX_IN_FT = 2;
    unique_ptr<FactorizedTable> factorizedTable;
    vector<map<table_id_t, ListUpdatesPerChunk>> listUpdatesPerTablePerDirection;
    unordered_map<uint32_t, uint32_t> propertyIDToColIdxMap;
    RelTableSchema relTableSchema;
};

} // namespace storage
} // namespace kuzu
