#pragma once

#include "src/catalog/include/catalog_structs.h"
#include "src/common/include/data_chunk/data_chunk.h"
#include "src/common/types/include/node_id_t.h"
#include "src/common/types/include/types.h"
#include "src/processor/include/physical_plan/result/factorized_table.h"
#include "src/storage/include/storage_utils.h"
#include "src/storage/storage_structure/include/lists/list_sync_state.h"

namespace graphflow {
namespace storage {

using namespace processor;
using namespace catalog;

using InsertedEdgeTupleIdxs = map<node_offset_t, vector<uint64_t>>;

class ListUpdateStore {

public:
    ListUpdateStore(MemoryManager& memoryManager, RelTableSchema& relTableSchema);

    inline uint32_t getColIdxInFT(uint32_t propertyID) const {
        return propertyIDToColIdxMap.at(propertyID);
    }
    inline bool isEmpty() const { return factorizedTable->getNumTuples() == 0; }
    inline map<uint64_t, InsertedEdgeTupleIdxs>& getInsertedEdgeTupleIdxes() {
        return insertedEdgeTupleIdxs;
    }
    inline FactorizedTable* getFactorizedTable() const { return factorizedTable.get(); }
    inline void clear() {
        factorizedTable->clear();
        insertedEdgeTupleIdxs.clear();
    }

    void addRel(vector<shared_ptr<ValueVector>>& srcDstNodeIDAndRelProperties);

    uint64_t getNumInsertedRelsForNodeOffset(node_offset_t nodeOffset) const;

    void readValues(
        ListSyncState& listSyncState, shared_ptr<ValueVector> valueVector, uint32_t colIdx) const;

private:
    unique_ptr<FactorizedTable> factorizedTable;
    shared_ptr<ValueVector> srcNodeVector;
    shared_ptr<ValueVector> dstNodeVector;
    shared_ptr<DataChunk> nodeDataChunk;
    map<uint64_t, InsertedEdgeTupleIdxs> insertedEdgeTupleIdxs;
    unordered_map<uint32_t, uint32_t> propertyIDToColIdxMap;
};

class RelUpdateStore {
public:
    RelUpdateStore(MemoryManager& memoryManager, RelTableSchema& relTableSchema)
        : memoryManager{memoryManager}, relTableSchema{relTableSchema} {}

    void addRel(vector<shared_ptr<ValueVector>> srcDstNodeIDAndRelProperties);

    shared_ptr<ListUpdateStore> getOrCreateListUpdateStore(
        RelDirection relDirection, table_id_t tableID);

private:
    void validateSrcDstNodeIDAndRelProperties(
        vector<shared_ptr<ValueVector>> srcDstNodeIDAndRelProperties) const;

private:
    vector<unordered_map<table_id_t, shared_ptr<ListUpdateStore>>> listUpdateStoresPerDirection{2};
    MemoryManager& memoryManager;
    RelTableSchema& relTableSchema;
};

} // namespace storage
} // namespace graphflow
