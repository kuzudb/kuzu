#pragma once

#include "src/catalog/include/catalog_structs.h"
#include "src/common/include/data_chunk/data_chunk.h"
#include "src/common/types/include/node_id_t.h"
#include "src/common/types/include/types.h"
#include "src/processor/result/include/factorized_table.h"
#include "src/storage/include/storage_utils.h"
#include "src/storage/storage_structure/include/lists/list_sync_state.h"

namespace graphflow {
namespace storage {

using namespace processor;
using namespace catalog;

using InsertedRelsPerNodeOffset = map<node_offset_t, vector<uint64_t>>;
using InsertedRelsPerChunk = map<uint64_t, InsertedRelsPerNodeOffset>;

struct InMemList;

/* AdjAndPropertyListsUpdateStore stores all inserted edges in a factorizedTable in the format:
 * [srcNodeID, dstNodeID, relProp1, relProp2, ..., relPropN]. In order to efficiently find the
 * insertedEdges for a nodeTable, we introduce a mapping which stores the tupleIdxes of
 * insertedEdges for each nodeTable per direction.
 */
class AdjAndPropertyListsUpdateStore {

public:
    AdjAndPropertyListsUpdateStore(MemoryManager& memoryManager, RelTableSchema& relTableSchema);

    inline bool isEmpty() const { return factorizedTable->isEmpty(); }
    inline void clear() {
        factorizedTable->clear();
        initInsertedRelsPerTableIDPerDirection();
    }
    inline InsertedRelsPerChunk& getInsertedRelsPerChunk(ListFileID& listFileID) {
        auto relNodeTableAndDir = getRelNodeTableAndDirFromListFileID(listFileID);
        return insertedRelsPerTableIDPerDirection[relNodeTableAndDir.dir].at(
            relNodeTableAndDir.srcNodeTableID);
    }
    inline vector<map<table_id_t, InsertedRelsPerChunk>>& getInsertedRelsPerTableIDPerDirection() {
        return insertedRelsPerTableIDPerDirection;
    }
    void readInsertionsToList(ListFileID& listFileID, vector<uint64_t> tupleIdxes,
        InMemList& inMemList, uint64_t numElementsInPersistentStore,
        DiskOverflowFile* diskOverflowFile, DataType dataType,
        NodeIDCompressionScheme* nodeIDCompressionScheme);

    void insertRelIfNecessary(vector<shared_ptr<ValueVector>>& srcDstNodeIDAndRelProperties);

    uint64_t getNumInsertedRelsForNodeOffset(
        ListFileID& listFileID, node_offset_t nodeOffset) const;

    void readValues(ListFileID& listFileID, ListSyncState& listSyncState,
        shared_ptr<ValueVector> valueVector) const;

private:
    static inline RelNodeTableAndDir getRelNodeTableAndDirFromListFileID(ListFileID& listFileID) {
        assert(listFileID.listType != ListType::UNSTRUCTURED_NODE_PROPERTY_LISTS);
        return listFileID.listType == ListType::ADJ_LISTS ?
                   listFileID.adjListsID.relNodeTableAndDir :
                   listFileID.relPropertyListID.relNodeTableAndDir;
    }
    uint32_t getColIdxInFT(ListFileID& listFileID) const;
    void validateSrcDstNodeIDAndRelProperties(
        vector<shared_ptr<ValueVector>> srcDstNodeIDAndRelProperties) const;
    void initInsertedRelsPerTableIDPerDirection();
    static void getErrorMsgForInvalidTableID(uint64_t tableID, bool isSrcTableID, string tableName);

private:
    unique_ptr<FactorizedTable> factorizedTable;
    shared_ptr<ValueVector> srcNodeVector;
    shared_ptr<ValueVector> dstNodeVector;
    shared_ptr<DataChunk> nodeDataChunk;
    vector<map<table_id_t, InsertedRelsPerChunk>> insertedRelsPerTableIDPerDirection;
    unordered_map<uint32_t, uint32_t> propertyIDToColIdxMap;
    RelTableSchema relTableSchema;
};

} // namespace storage
} // namespace graphflow
