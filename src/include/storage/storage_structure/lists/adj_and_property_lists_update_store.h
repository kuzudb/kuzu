#pragma once

#include "catalog/catalog_structs.h"
#include "common/data_chunk/data_chunk.h"
#include "common/types/node_id_t.h"
#include "common/types/types.h"
#include "processor/result/factorized_table.h"
#include "storage/storage_structure/lists/list_sync_state.h"
#include "storage/storage_utils.h"

namespace kuzu {
namespace storage {

using namespace processor;
using namespace catalog;

struct ListUpdates {
    ListUpdates() : emptyListInPersistentStore{false} {}
    bool emptyListInPersistentStore;
    vector<uint64_t> insertedRelsTupleIdxInFT;
};

using ListUpdatesPerNode = map<node_offset_t, ListUpdates>;
using ListUpdatesPerChunk = map<uint64_t, ListUpdatesPerNode>;

struct InMemList;

/* ListsUpdateStore stores all inserted edges in a factorizedTable in the format:
 * [srcNodeID, dstNodeID, relProp1, relProp2, ..., relPropN]. In order to efficiently find the
 * insertedEdges for a nodeTable, we introduce a mapping which stores the tupleIdxes of
 * insertedEdges for each nodeTable per direction.
 */
class ListsUpdateStore {

public:
    ListsUpdateStore(MemoryManager& memoryManager, RelTableSchema& relTableSchema);

    inline bool isEmpty() const { return factorizedTable->isEmpty(); }
    inline void clear() {
        factorizedTable->clear();
        initListUpdatesPerTablePerDirection();
    }
    inline ListUpdatesPerChunk& getListUpdatesPerChunk(ListFileID& listFileID) {
        auto relNodeTableAndDir = getRelNodeTableAndDirFromListFileID(listFileID);
        return listUpdatesPerTablePerDirection[relNodeTableAndDir.dir].at(
            relNodeTableAndDir.srcNodeTableID);
    }
    inline vector<map<table_id_t, ListUpdatesPerChunk>>& getListUpdatesPerTablePerDirection() {
        return listUpdatesPerTablePerDirection;
    }

    bool isListEmptyInPersistentStore(ListFileID& listFileID, node_offset_t nodeOffset) const;

    bool hasUpdates() const;

    void readInsertionsToList(ListFileID& listFileID, vector<uint64_t> tupleIdxes,
        InMemList& inMemList, uint64_t numElementsInPersistentStore,
        DiskOverflowFile* diskOverflowFile, DataType dataType,
        NodeIDCompressionScheme* nodeIDCompressionScheme);

    // If this is a one-to-one relTable, all properties are stored in columns.
    // In this case, the listsUpdateStore should not store the insert rels in FT.
    void insertRelIfNecessary(shared_ptr<ValueVector>& srcNodeIDVector,
        shared_ptr<ValueVector>& dstNodeIDVector,
        vector<shared_ptr<ValueVector>>& relPropertyVectors);

    uint64_t getNumInsertedRelsForNodeOffset(
        ListFileID& listFileID, node_offset_t nodeOffset) const;

    void readValues(ListFileID& listFileID, ListSyncState& listSyncState,
        shared_ptr<ValueVector> valueVector) const;

    void initEmptyListInPersistentStore(nodeID_t& nodeID) {
        for (auto direction : REL_DIRECTIONS) {
            if (listUpdatesPerTablePerDirection[direction].contains(nodeID.tableID)) {
                listUpdatesPerTablePerDirection[direction][nodeID.tableID]
                                               [StorageUtils::getListChunkIdx(nodeID.offset)]
                                               [nodeID.offset]
                                                   .emptyListInPersistentStore = true;
            }
        }
    }

private:
    static inline RelNodeTableAndDir getRelNodeTableAndDirFromListFileID(ListFileID& listFileID) {
        return listFileID.listType == ListType::ADJ_LISTS ?
                   listFileID.adjListsID.relNodeTableAndDir :
                   listFileID.relPropertyListID.relNodeTableAndDir;
    }
    uint32_t getColIdxInFT(ListFileID& listFileID) const;
    void initListUpdatesPerTablePerDirection();

private:
    unique_ptr<FactorizedTable> factorizedTable;
    shared_ptr<ValueVector> srcNodeVector;
    shared_ptr<ValueVector> dstNodeVector;
    shared_ptr<DataChunk> nodeDataChunk;
    vector<map<table_id_t, ListUpdatesPerChunk>> listUpdatesPerTablePerDirection;
    unordered_map<uint32_t, uint32_t> propertyIDToColIdxMap;
    RelTableSchema relTableSchema;
};

} // namespace storage
} // namespace kuzu
