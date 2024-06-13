#pragma once

#include <map>

#include "common/types/internal_id_t.h"
#include "common/vector/value_vector.h"
#include "storage/store/chunked_node_group.h"
#include "storage/store/chunked_node_group_collection.h"

namespace kuzu {
namespace storage {

using offset_to_row_idx_t = std::map<common::offset_t, common::row_idx_t>;
using offset_to_row_idx_vec_t = std::map<common::offset_t, std::vector<common::row_idx_t>>;
using offset_set_t = std::unordered_set<common::offset_t>;

static constexpr common::column_id_t NBR_ID_COLUMN_ID = 0;
static constexpr common::column_id_t REL_ID_COLUMN_ID = 1;

using ChunkCollection = std::vector<ColumnChunkData*>;

class LocalChunkedGroupCollection {
public:
    explicit LocalChunkedGroupCollection(std::vector<common::LogicalType> dataTypes)
        : dataTypes{std::move(dataTypes)},
          chunkedGroups{common::LogicalType::copy(this->dataTypes)}, numRows{0} {}
    DELETE_COPY_DEFAULT_MOVE(LocalChunkedGroupCollection);

    static std::pair<uint32_t, uint64_t> getChunkIdxAndOffsetInChunk(common::row_idx_t rowIdx) {
        return std::make_pair(rowIdx / ChunkedNodeGroupCollection::CHUNK_CAPACITY,
            rowIdx % ChunkedNodeGroupCollection::CHUNK_CAPACITY);
    }

    common::row_idx_t getRowIdxFromOffset(common::offset_t offset) const {
        KU_ASSERT(offsetToRowIdx.contains(offset));
        return offsetToRowIdx.at(offset);
    }
    const std::vector<common::row_idx_t>& getRelOffsetsFromSrcOffset(
        common::offset_t srcOffset) const {
        KU_ASSERT(srcNodeOffsetToRelOffsets.contains(srcOffset));
        return srcNodeOffsetToRelOffsets.at(srcOffset);
    }
    bool hasOffset(common::offset_t offset) const { return offsetToRowIdx.contains(offset); }
    bool hasRelOffsetsFromSrcOffset(common::offset_t srcOffset) const {
        return srcNodeOffsetToRelOffsets.contains(srcOffset);
    }
    uint64_t getNumRelsFromSrcOffset(common::offset_t srcOffset) const {
        return srcNodeOffsetToRelOffsets.at(srcOffset).size();
    }
    const offset_to_row_idx_vec_t& getSrcNodeOffsetToRelOffsets() const {
        return srcNodeOffsetToRelOffsets;
    }
    const offset_to_row_idx_t& getOffsetToRowIdx() const { return offsetToRowIdx; }

    void appendChunkedGroup(ColumnChunkData* srcOffsetChunk,
        std::unique_ptr<ChunkedNodeGroup> chunkedGroup);

    bool isEmpty() const { return offsetToRowIdx.empty() && srcNodeOffsetToRelOffsets.empty(); }
    void readValueAtRowIdx(common::row_idx_t rowIdx, common::column_id_t columnID,
        common::ValueVector* outputVector, common::sel_t posInOutputVector) const;
    bool read(common::offset_t offset, const std::vector<common::column_id_t>& columnIDs,
        const std::vector<common::ValueVector*>& outputVector,
        common::offset_t offsetInOutputVector) const;
    bool read(common::offset_t offset, common::column_id_t columnID,
        common::ValueVector* outputVector, common::sel_t posInOutputVector) const;

    ChunkedNodeGroup* getLastChunkedGroupAndAddNewGroupIfNecessary();
    void append(common::offset_t offset, const std::vector<common::ValueVector*>& vectors) {
        offsetToRowIdx[offset] = append(vectors);
    }
    void append(common::offset_t offset, ChunkedNodeGroup* nodeGroup, common::offset_t numValues);
    // Only used for rel tables. Should be moved out later.
    void append(common::offset_t nodeOffset, common::offset_t relOffset,
        const std::vector<common::ValueVector*>& vectors) {
        append(relOffset, vectors);
        srcNodeOffsetToRelOffsets[nodeOffset].push_back(relOffset);
    }
    void update(common::offset_t offset, common::column_id_t columnID,
        common::ValueVector* propertyVector);
    void remove(common::offset_t offset) {
        if (offsetToRowIdx.contains(offset)) {
            offsetToRowIdx.erase(offset);
        }
    }
    // Only used for rel tables. Should be moved out later.
    void remove(common::offset_t srcNodeOffset, common::offset_t relOffset);

    ChunkCollection getLocalChunk(common::column_id_t columnID) const {
        ChunkCollection localChunkCollection;
        for (auto& chunkedGroup : chunkedGroups.getChunkedGroups()) {
            localChunkCollection.push_back(&chunkedGroup->getColumnChunkUnsafe(columnID));
        }
        return localChunkCollection;
    }

private:
    common::row_idx_t append(const std::vector<common::ValueVector*>& vectors);

private:
    std::vector<common::LogicalType> dataTypes;
    ChunkedNodeGroupCollection chunkedGroups;
    // The offset here can either be nodeOffset ( for node table) or relOffset (for rel table).
    offset_to_row_idx_t offsetToRowIdx;
    common::row_idx_t numRows;

    // Only used for rel tables. Should be moved out later.
    offset_to_row_idx_vec_t srcNodeOffsetToRelOffsets;
};

class LocalDeletionInfo {
public:
    bool isEmpty() const { return deletedOffsets.empty() && srcNodeOffsetToRelOffsetVec.empty(); }
    bool isEmpty(common::offset_t srcOffset) const {
        return !srcNodeOffsetToRelOffsetVec.contains(srcOffset) ||
               srcNodeOffsetToRelOffsetVec.at(srcOffset).empty();
    }
    bool containsOffset(common::offset_t offset) const { return deletedOffsets.contains(offset); }
    bool deleteOffset(common::offset_t offset) {
        if (deletedOffsets.contains(offset)) {
            return false;
        }
        deletedOffsets.insert(offset);
        return true;
    }
    void clearNodeOffset(common::offset_t offset) {
        KU_ASSERT(containsOffset(offset));
        deletedOffsets.erase(offset);
    }

    // For rel tables only.
    void deleteRelAux(common::offset_t srcNodeOffset, common::offset_t relOffset) {
        srcNodeOffsetToRelOffsetVec[srcNodeOffset].push_back(relOffset);
    }
    const offset_to_row_idx_vec_t& getSrcNodeOffsetToRelOffsetVec() const {
        return srcNodeOffsetToRelOffsetVec;
    }
    uint64_t getNumDeletedRelsFromSrcOffset(common::offset_t srcOffset) const {
        return srcNodeOffsetToRelOffsetVec.contains(srcOffset) ?
                   srcNodeOffsetToRelOffsetVec.at(srcOffset).size() :
                   0;
    }

private:
    // The offset here can either be nodeOffset ( for node table) or relOffset (for rel table).
    offset_set_t deletedOffsets;

    // Only used for rel tables. Should be moved out later.
    offset_to_row_idx_vec_t srcNodeOffsetToRelOffsetVec;
};

class LocalNodeGroup {
public:
    LocalNodeGroup(common::offset_t nodeGroupStartOffset,
        const std::vector<common::LogicalType>& dataTypes);
    DELETE_COPY_DEFAULT_MOVE(LocalNodeGroup);
    virtual ~LocalNodeGroup() = default;

    virtual bool insert(std::vector<common::ValueVector*> nodeIDVectors,
        std::vector<common::ValueVector*> propertyVectors) = 0;
    virtual bool update(std::vector<common::ValueVector*> nodeIDVectors,
        common::column_id_t columnID, common::ValueVector* propertyVector) = 0;
    virtual bool delete_(common::ValueVector* IDVector, common::ValueVector* extraVector) = 0;

    const LocalChunkedGroupCollection& getUpdateChunks(common::column_id_t columnID) const {
        KU_ASSERT(columnID < updateChunks.size());
        return updateChunks[columnID];
    }
    LocalChunkedGroupCollection& getUpdateChunks(common::column_id_t columnID) {
        KU_ASSERT(columnID < updateChunks.size());
        return updateChunks[columnID];
    }
    LocalChunkedGroupCollection& getInsertChunks() { return insertChunks; }

    bool hasUpdatesOrDeletions() const;

protected:
    common::offset_t nodeGroupStartOffset;

    LocalChunkedGroupCollection insertChunks;
    LocalDeletionInfo deleteInfo;
    std::vector<LocalChunkedGroupCollection> updateChunks;
};

class LocalTableData {
    friend class NodeTable;
    friend class NodeTableData;

public:
    explicit LocalTableData(common::table_id_t tableID, std::vector<common::LogicalType> dataTypes)
        : tableID{tableID}, dataTypes{std::move(dataTypes)} {}
    virtual ~LocalTableData() = default;

    void clear() { nodeGroups.clear(); }

    bool insert(std::vector<common::ValueVector*> nodeIDVectors,
        std::vector<common::ValueVector*> propertyVectors);
    bool update(std::vector<common::ValueVector*> nodeIDVectors, common::column_id_t columnID,
        common::ValueVector* propertyVector);
    bool delete_(common::ValueVector* nodeIDVector, common::ValueVector* extraVector = nullptr);

    std::vector<LocalNodeGroup*> getNodeGroups() const {
        std::vector<LocalNodeGroup*> nodeGroups;
        for (const auto& [_, nodeGroup] : this->nodeGroups) {
            nodeGroups.push_back(nodeGroup.get());
        }
        return nodeGroups;
    }

protected:
    virtual LocalNodeGroup* getOrCreateLocalNodeGroup(common::ValueVector* nodeIDVector) = 0;

protected:
    common::table_id_t tableID;
    std::vector<common::LogicalType> dataTypes;

    std::map<common::node_group_idx_t, std::unique_ptr<LocalNodeGroup>> nodeGroups;
};

struct TableInsertState;
struct TableUpdateState;
struct TableDeleteState;
class Table;
class LocalTable {
public:
    virtual ~LocalTable() = default;

    virtual bool insert(TableInsertState& insertState) = 0;
    virtual bool update(TableUpdateState& updateState) = 0;
    virtual bool delete_(TableDeleteState& deleteState) = 0;

    void clear() { localTableDataCollection.clear(); }

protected:
    explicit LocalTable(Table& table) : table{table} {}

protected:
    Table& table;
    // For a node table, it should only contain one LocalTableData, while a rel table should contain
    // two, one for each direction.
    std::vector<std::unique_ptr<LocalTableData>> localTableDataCollection;
};

} // namespace storage
} // namespace kuzu
