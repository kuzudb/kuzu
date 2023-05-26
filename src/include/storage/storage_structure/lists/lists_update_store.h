#pragma once

#include <map>

#include "catalog/catalog_structs.h"
#include "common/data_chunk/data_chunk.h"
#include "common/rel_direction.h"
#include "common/types/types.h"
#include "processor/result/factorized_table.h"
#include "storage/storage_structure/lists/list_handle.h"
#include "storage/storage_utils.h"

namespace kuzu {
namespace storage {

typedef uint64_t list_offset_t;

struct UpdatedPersistentListOffsets {
public:
    inline void insertOffset(list_offset_t listOffset, processor::ft_tuple_idx_t ftTupleIdx) {
        listOffsetFTIdxMap[listOffset] = ftTupleIdx;
    }
    inline bool hasUpdates() const { return !listOffsetFTIdxMap.empty(); }

public:
    std::map<list_offset_t, processor::ft_tuple_idx_t> listOffsetFTIdxMap;
};

struct ListsUpdatesForNodeOffset {
public:
    explicit ListsUpdatesForNodeOffset(const catalog::RelTableSchema& relTableSchema);

    inline bool hasUpdates() const {
        return isNewlyAddedNode || !insertedRelsTupleIdxInFT.empty() ||
               !deletedRelOffsets.empty() || hasAnyUpdatedPersistentListOffsets();
    }

    bool hasAnyUpdatedPersistentListOffsets() const;

public:
    bool isNewlyAddedNode;
    std::vector<processor::ft_tuple_idx_t> insertedRelsTupleIdxInFT;
    std::unordered_map<common::property_id_t, UpdatedPersistentListOffsets>
        updatedPersistentListOffsets;
    std::unordered_set<common::offset_t> deletedRelOffsets;
};

struct ListsUpdateInfo {
public:
    ListsUpdateInfo(common::ValueVector* propertyVector, common::property_id_t propertyID,
        common::offset_t relOffset, uint64_t fwdListOffset, uint64_t bwdListOffset)
        : propertyVector{propertyVector}, propertyID{propertyID}, relOffset{relOffset},
          fwdListOffset{fwdListOffset}, bwdListOffset{bwdListOffset} {}

    bool isStoredInPersistentStore() const { return fwdListOffset != -1 || bwdListOffset != -1; }

public:
    common::ValueVector* propertyVector;
    common::property_id_t propertyID;
    common::offset_t relOffset;
    list_offset_t fwdListOffset;
    list_offset_t bwdListOffset;
};

// Shouldn't need to be shared, except that MSVC doesn't allow maps inside vectors to have
// non-copyable values.
using ListsUpdatesPerNode = std::map<common::offset_t, std::shared_ptr<ListsUpdatesForNodeOffset>>;
using ListsUpdatesPerChunk = std::map<chunk_idx_t, ListsUpdatesPerNode>;

struct InMemList;

class ListsUpdatesStore {

public:
    ListsUpdatesStore(MemoryManager& memoryManager, catalog::RelTableSchema& relTableSchema);

    inline void clear() {
        ftOfInsertedRels->clear();
        for (auto& [_, propertyUpdates] : listsUpdates) {
            propertyUpdates->clear();
        }
        initListsUpdatesPerTablePerDirection();
    }
    inline ListsUpdatesPerChunk& getListsUpdatesPerChunk(
        common::RelDataDirection relDataDirection) {
        return listsUpdatesPerDirection[relDataDirection];
    }

    void updateSchema(catalog::RelTableSchema& relTableSchema);

    bool isNewlyAddedNode(ListFileID& listFileID, common::offset_t nodeOffset) const;

    uint64_t getNumDeletedRels(ListFileID& listFileID, common::offset_t nodeOffset) const;

    bool isRelDeletedInPersistentStore(
        ListFileID& listFileID, common::offset_t nodeOffset, common::offset_t relOffset) const;

    bool hasUpdates() const;

    void readInsertedRelsToList(ListFileID& listFileID,
        std::vector<processor::ft_tuple_idx_t> tupleIdxes, InMemList& inMemList,
        uint64_t numElementsInPersistentStore, DiskOverflowFile* diskOverflowFile,
        common::LogicalType dataType);

    // If this is a one-to-one relTable, all properties are stored in columns.
    // In this case, the listsUpdatesStore should not store the insert rels in FT.
    void insertRelIfNecessary(const common::ValueVector* srcNodeIDVector,
        const common::ValueVector* dstNodeIDVector,
        const std::vector<common::ValueVector*>& relPropertyVectors);

    void deleteRelIfNecessary(common::ValueVector* srcNodeIDVector,
        common::ValueVector* dstNodeIDVector, common::ValueVector* relIDVector);

    uint64_t getNumInsertedRelsForNodeOffset(
        ListFileID& listFileID, common::offset_t nodeOffset) const;

    void readValues(
        ListFileID& listFileID, ListHandle& listSyncState, common::ValueVector* valueVector) const;

    bool hasAnyDeletedRelsInPersistentStore(
        ListFileID& listFileID, common::offset_t nodeOffset) const;

    // This function is called ifNecessary because it only handles the updates to a propertyList.
    // If the property is stored as a column in both direction(e.g. we are updating a ONE-ONE rel
    // table), this function won't do any operations in this case.
    void updateRelIfNecessary(common::ValueVector* srcNodeIDVector,
        common::ValueVector* dstNodeIDVector, const ListsUpdateInfo& listsUpdateInfo);

    void readUpdatesToPropertyVectorIfExists(ListFileID& listFileID, common::offset_t nodeOffset,
        common::ValueVector* propertyVector, list_offset_t startListOffset);

    void readPropertyUpdateToInMemList(ListFileID& listFileID, processor::ft_tuple_idx_t ftTupleIdx,
        InMemList& inMemList, uint64_t posToWriteToInMemList, const common::LogicalType& dataType,
        DiskOverflowFile* overflowFileOfInMemList);

    void initNewlyAddedNodes(common::nodeID_t& nodeID);

private:
    static inline RelNodeTableAndDir getRelNodeTableAndDirFromListFileID(ListFileID& listFileID) {
        return listFileID.listType == ListType::ADJ_LISTS ?
                   listFileID.adjListsID.relNodeTableAndDir :
                   listFileID.relPropertyListID.relNodeTableAndDir;
    }
    inline int64_t getTupleIdxIfInsertedRel(int64_t relID) {
        return ftOfInsertedRels->findValueInFlatColumn(INTERNAL_REL_ID_IDX_IN_FT, relID);
    }

    void initInsertedRelsAndListsUpdates();

    processor::ft_col_idx_t getColIdxInFT(ListFileID& listFileID) const;

    void initListsUpdatesPerTablePerDirection();

    ListsUpdatesForNodeOffset* getOrCreateListsUpdatesForNodeOffset(
        common::RelDataDirection relDirection, common::nodeID_t nodeID);

    ListsUpdatesForNodeOffset* getListsUpdatesForNodeOffsetIfExists(
        ListFileID& listFileID, common::offset_t nodeOffset) const;

private:
    /* ListsUpdatesStore stores all inserted edges in a factorizedTable in the format:
     * [srcNodeID, dstNodeID, relID, relProp1, relProp2, ..., relPropN]. In order to efficiently
     * find the insertedEdges for a nodeTable, we introduce a mapping which stores the tupleIdxes of
     * insertedEdges for each nodeTable per direction.
     */
    static constexpr processor::ft_tuple_idx_t SRC_TABLE_ID_IDX_IN_FT = 0;
    static constexpr processor::ft_tuple_idx_t DST_TABLE_ID_IDX_IN_FT = 1;
    static constexpr processor::ft_tuple_idx_t INTERNAL_REL_ID_IDX_IN_FT = 2;
    static constexpr processor::ft_tuple_idx_t LISTS_UPDATES_IDX_IN_FT = 0;
    std::unique_ptr<processor::FactorizedTable> ftOfInsertedRels;
    // We store all updates to the same property list inside a special factorizedTable which
    // only has one column. ListsUpdates is a collection of these special factorizedTable indexed by
    // propertyID.
    std::unordered_map<common::property_id_t, std::unique_ptr<processor::FactorizedTable>>
        listsUpdates;
    std::vector<ListsUpdatesPerChunk> listsUpdatesPerDirection;
    std::unordered_map<common::property_id_t, processor::ft_col_idx_t> propertyIDToColIdxMap;
    catalog::RelTableSchema relTableSchema;
    MemoryManager& memoryManager;
};

} // namespace storage
} // namespace kuzu
