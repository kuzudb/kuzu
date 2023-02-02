#pragma once

#include <map>

#include "catalog/catalog_structs.h"
#include "common/data_chunk/data_chunk.h"
#include "common/types/types.h"
#include "processor/result/factorized_table.h"
#include "storage/storage_structure/lists/list_handle.h"
#include "storage/storage_utils.h"

namespace kuzu {
namespace storage {

using namespace processor;
using namespace catalog;

typedef uint64_t list_offset_t;

struct UpdatedPersistentListOffsets {
public:
    inline void insertOffset(list_offset_t listOffset, ft_tuple_idx_t ftTupleIdx) {
        listOffsetFTIdxMap[listOffset] = ftTupleIdx;
    }
    inline bool hasUpdates() const { return !listOffsetFTIdxMap.empty(); }

public:
    map<list_offset_t, ft_tuple_idx_t> listOffsetFTIdxMap;
};

struct ListsUpdatesForNodeOffset {
public:
    explicit ListsUpdatesForNodeOffset(const RelTableSchema& relTableSchema);

    inline bool hasUpdates() const {
        return isNewlyAddedNode || !insertedRelsTupleIdxInFT.empty() ||
               !deletedRelOffsets.empty() || hasAnyUpdatedPersistentListOffsets();
    }

    bool hasAnyUpdatedPersistentListOffsets() const;

public:
    bool isNewlyAddedNode;
    vector<ft_tuple_idx_t> insertedRelsTupleIdxInFT;
    unordered_map<property_id_t, UpdatedPersistentListOffsets> updatedPersistentListOffsets;
    unordered_set<offset_t> deletedRelOffsets;
};

struct ListsUpdateInfo {
public:
    ListsUpdateInfo(const shared_ptr<ValueVector>& propertyVector, property_id_t propertyID,
        int64_t relID, uint64_t fwdListOffset, uint64_t bwdListOffset)
        : propertyVector{propertyVector}, propertyID{propertyID}, relID{relID},
          fwdListOffset{fwdListOffset}, bwdListOffset{bwdListOffset} {}

    bool isStoredInPersistentStore() const { return fwdListOffset != -1 || bwdListOffset != -1; }

public:
    shared_ptr<ValueVector> propertyVector;
    property_id_t propertyID;
    int64_t relID;
    list_offset_t fwdListOffset;
    list_offset_t bwdListOffset;
};

using ListsUpdatesPerNode = map<offset_t, unique_ptr<ListsUpdatesForNodeOffset>>;
using ListsUpdatesPerChunk = map<chunk_idx_t, ListsUpdatesPerNode>;

struct InMemList;

class ListsUpdatesStore {

public:
    ListsUpdatesStore(MemoryManager& memoryManager, RelTableSchema& relTableSchema);

    inline void clear() {
        ftOfInsertedRels->clear();
        for (auto& [_, propertyUpdates] : listsUpdates) {
            propertyUpdates->clear();
        }
        initListsUpdatesPerTablePerDirection();
    }
    inline map<table_id_t, ListsUpdatesPerChunk>& getListsUpdatesPerBoundNodeTableOfDirection(
        RelDirection relDirection) {
        return listsUpdatesPerTablePerDirection[relDirection];
    }

    bool isNewlyAddedNode(ListFileID& listFileID, offset_t nodeOffset) const;

    uint64_t getNumDeletedRels(ListFileID& listFileID, offset_t nodeOffset) const;

    bool isRelDeletedInPersistentStore(
        ListFileID& listFileID, offset_t nodeOffset, offset_t relOffset) const;

    bool hasUpdates() const;

    void readInsertedRelsToList(ListFileID& listFileID, vector<ft_tuple_idx_t> tupleIdxes,
        InMemList& inMemList, uint64_t numElementsInPersistentStore,
        DiskOverflowFile* diskOverflowFile, DataType dataType,
        NodeIDCompressionScheme* nodeIDCompressionScheme);

    // If this is a one-to-one relTable, all properties are stored in columns.
    // In this case, the listsUpdatesStore should not store the insert rels in FT.
    void insertRelIfNecessary(const shared_ptr<ValueVector>& srcNodeIDVector,
        const shared_ptr<ValueVector>& dstNodeIDVector,
        const vector<shared_ptr<ValueVector>>& relPropertyVectors);

    void deleteRelIfNecessary(const shared_ptr<ValueVector>& srcNodeIDVector,
        const shared_ptr<ValueVector>& dstNodeIDVector, const shared_ptr<ValueVector>& relIDVector);

    uint64_t getNumInsertedRelsForNodeOffset(ListFileID& listFileID, offset_t nodeOffset) const;

    void readValues(ListFileID& listFileID, ListHandle& listSyncState,
        shared_ptr<ValueVector> valueVector) const;

    bool hasAnyDeletedRelsInPersistentStore(ListFileID& listFileID, offset_t nodeOffset) const;

    // This function is called ifNecessary because it only handles the updates to a propertyList.
    // If the property is stored as a column in both direction(e.g. we are updating a ONE-ONE rel
    // table), this function won't do any operations in this case.
    void updateRelIfNecessary(const shared_ptr<ValueVector>& srcNodeIDVector,
        const shared_ptr<ValueVector>& dstNodeIDVector, const ListsUpdateInfo& listsUpdateInfo);

    void readUpdatesToPropertyVectorIfExists(ListFileID& listFileID, offset_t nodeOffset,
        const shared_ptr<ValueVector>& valueVector, list_offset_t startListOffset);

    void readPropertyUpdateToInMemList(ListFileID& listFileID, ft_tuple_idx_t ftTupleIdx,
        InMemList& inMemList, uint64_t posToWriteToInMemList, const DataType& dataType,
        DiskOverflowFile* overflowFileOfInMemList);

    void initNewlyAddedNodes(nodeID_t& nodeID);

private:
    static inline RelNodeTableAndDir getRelNodeTableAndDirFromListFileID(ListFileID& listFileID) {
        return listFileID.listType == ListType::ADJ_LISTS ?
                   listFileID.adjListsID.relNodeTableAndDir :
                   listFileID.relPropertyListID.relNodeTableAndDir;
    }
    inline int64_t getTupleIdxIfInsertedRel(int64_t relID) {
        return ftOfInsertedRels->findValueInFlatColumn(INTERNAL_REL_ID_IDX_IN_FT, relID);
    }

    void initInsertedRels();

    ft_col_idx_t getColIdxInFT(ListFileID& listFileID) const;

    void initListsUpdatesPerTablePerDirection();

    ListsUpdatesForNodeOffset* getOrCreateListsUpdatesForNodeOffset(
        RelDirection relDirection, nodeID_t nodeID);

    ListsUpdatesForNodeOffset* getListsUpdatesForNodeOffsetIfExists(
        ListFileID& listFileID, offset_t nodeOffset) const;

private:
    /* ListsUpdatesStore stores all inserted edges in a factorizedTable in the format:
     * [srcNodeID, dstNodeID, relID, relProp1, relProp2, ..., relPropN]. In order to efficiently
     * find the insertedEdges for a nodeTable, we introduce a mapping which stores the tupleIdxes of
     * insertedEdges for each nodeTable per direction.
     */
    static constexpr ft_col_idx_t SRC_TABLE_ID_IDX_IN_FT = 0;
    static constexpr ft_col_idx_t DST_TABLE_ID_IDX_IN_FT = 1;
    static constexpr ft_col_idx_t INTERNAL_REL_ID_IDX_IN_FT = 2;
    static constexpr ft_col_idx_t LISTS_UPDATES_IDX_IN_FT = 0;
    unique_ptr<FactorizedTable> ftOfInsertedRels;
    // We store all updates to the same property list inside a special factorizedTable which
    // only has one column. ListsUpdates is a collection of these special factorizedTable indexed by
    // propertyID.
    unordered_map<property_id_t, unique_ptr<FactorizedTable>> listsUpdates;
    vector<map<table_id_t, ListsUpdatesPerChunk>> listsUpdatesPerTablePerDirection;
    unordered_map<property_id_t, ft_col_idx_t> propertyIDToColIdxMap;
    RelTableSchema relTableSchema;
    MemoryManager& memoryManager;
};

} // namespace storage
} // namespace kuzu
