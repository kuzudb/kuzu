#pragma once

#include "common/column_data_format.h"
#include "common/vector/value_vector.h"
#include "storage/local_storage/local_table.h"

namespace kuzu {
namespace storage {

static constexpr common::column_id_t REL_ID_COLUMN_ID = 0;

struct RelNGInfo {
    virtual ~RelNGInfo() = default;

    virtual bool insert(common::offset_t srcOffsetInChunk, common::offset_t relOffset,
        common::row_idx_t adjNodeRowIdx,
        const std::vector<common::row_idx_t>& propertyNodesRowIdx) = 0;
    virtual void update(common::offset_t srcOffsetInChunk, common::offset_t relOffset,
        common::column_id_t columnID, common::row_idx_t rowIdx) = 0;
    virtual bool delete_(common::offset_t srcOffsetInChunk, common::offset_t relOffset) = 0;

    virtual uint64_t getNumInsertedTuples(common::offset_t srcOffsetInChunk) = 0;

protected:
    inline static bool contains(
        const std::unordered_set<common::offset_t>& set, common::offset_t value) {
        return set.find(value) != set.end();
    }
};

// Info of node groups with regular chunks for rel tables.
// Note that srcNodeOffset here are the relative offset within each node group.
struct RegularRelNGInfo final : public RelNGInfo {
    // Note that adj chunk cannot be directly updated. It can only be inserted or deleted.
    offset_to_row_idx_t adjInsertInfo;                   // insert info for adj chunk.
    std::vector<offset_to_row_idx_t> insertInfoPerChunk; // insert info for property chunks.
    std::vector<offset_to_row_idx_t> updateInfoPerChunk; // insert info for property chunks.
    offset_set_t deleteInfo;                             // the set of deleted node offsets.

    RegularRelNGInfo(common::column_id_t numChunks) {
        insertInfoPerChunk.resize(numChunks);
        updateInfoPerChunk.resize(numChunks);
    }

    bool insert(common::offset_t srcOffsetInChunk, common::offset_t relOffset,
        common::row_idx_t adjNodeRowIdx,
        const std::vector<common::row_idx_t>& propertyNodesRowIdx) override;
    void update(common::offset_t srcOffsetInChunk, common::offset_t relOffset,
        common::column_id_t columnID, common::row_idx_t rowIdx) override;
    bool delete_(common::offset_t srcOffsetInChunk, common::offset_t relOffset) override;

    uint64_t getNumInsertedTuples(common::offset_t srcOffsetInChunk) override;
};

// Info of node groups with CSR chunks for rel tables.
// Note that srcNodeOffset here are the relative offset within each node group.
struct CSRRelNGInfo final : public RelNGInfo {
    offset_to_offset_to_row_idx_t adjInsertInfo;
    std::vector<offset_to_offset_to_row_idx_t> insertInfoPerChunk;
    std::vector<offset_to_offset_to_row_idx_t> updateInfoPerChunk;
    offset_to_offset_set_t deleteInfo;

    CSRRelNGInfo(common::column_id_t numChunks) {
        insertInfoPerChunk.resize(numChunks);
        updateInfoPerChunk.resize(numChunks);
    }

    bool insert(common::offset_t srcOffsetInChunk, common::offset_t relOffset,
        common::row_idx_t adjNodeRowIdx,
        const std::vector<common::row_idx_t>& propertyNodesRowIdx) override;
    void update(common::offset_t srcOffsetInChunk, common::offset_t relOffset,
        common::column_id_t columnID, common::row_idx_t rowIdx) override;
    bool delete_(common::offset_t srcOffsetInChunk, common::offset_t relOffset) override;

    uint64_t getNumInsertedTuples(common::offset_t srcOffsetInChunk) override;
};

class LocalRelNG final : public LocalNodeGroup {
public:
    LocalRelNG(common::offset_t nodeGroupStartOffset, common::ColumnDataFormat dataFormat,
        std::vector<common::LogicalType*> dataTypes, MemoryManager* mm);

    common::row_idx_t scanCSR(common::offset_t srcOffsetInChunk,
        common::offset_t posToReadForOffset, const std::vector<common::column_id_t>& columnIDs,
        const std::vector<common::ValueVector*>& outputVector);
    // For CSR, we need to apply updates and deletions here, while insertions are handled by
    // `scanCSR`.
    void applyLocalChangesForCSRColumns(common::offset_t srcOffsetInChunk,
        const std::vector<common::column_id_t>& columnIDs, common::ValueVector* relIDVector,
        const std::vector<common::ValueVector*>& outputVector);
    void applyLocalChangesForRegularColumns(common::ValueVector* srcNodeIDVector,
        const std::vector<common::column_id_t>& columnIDs,
        const std::vector<common::ValueVector*>& outputVector);
    // Note that there is an implicit assumption that all outputVectors share the same state, thus
    // only one posInVector is passed.
    void applyLocalChangesForRegularColumns(common::offset_t offsetInChunk,
        const std::vector<common::column_id_t>& columnIDs,
        const std::vector<common::ValueVector*>& outputVectors, common::sel_t posInVector);

    bool insert(common::ValueVector* srcNodeIDVector, common::ValueVector* dstNodeIDVector,
        const std::vector<common::ValueVector*>& propertyVectors);
    void update(common::ValueVector* srcNodeIDVector, common::ValueVector* relIDVector,
        common::column_id_t columnID, common::ValueVector* propertyVector);
    bool delete_(common::ValueVector* srcNodeIDVector, common::ValueVector* relIDVector);

    inline LocalVectorCollection* getAdjChunk() { return adjChunk.get(); }
    inline LocalVectorCollection* getPropertyChunk(common::column_id_t columnID) {
        KU_ASSERT(columnID < chunks.size());
        return chunks[columnID].get();
    }
    inline RelNGInfo* getRelNGInfo() { return relNGInfo.get(); }

private:
    void applyCSRUpdates(common::offset_t srcOffsetInChunk, common::column_id_t columnID,
        const offset_to_offset_to_row_idx_t& updateInfo, common::ValueVector* relIDVector,
        const std::vector<common::ValueVector*>& outputVector);
    void applyCSRDeletions(common::offset_t srcOffsetInChunk,
        const offset_to_offset_set_t& deleteInfo, common::ValueVector* relIDVector);
    void applyRegularChangesToVector(common::ValueVector* srcNodeIDVector,
        LocalVectorCollection* chunk, const offset_to_row_idx_t& updateInfo,
        const offset_to_row_idx_t& insertInfo, const offset_set_t& deleteInfo,
        common::ValueVector* outputVector);
    void applyRegularChangesForOffset(common::offset_t offsetInChunk, LocalVectorCollection* chunk,
        const offset_to_row_idx_t& updateInfo, const offset_to_row_idx_t& insertInfo,
        const offset_set_t& deleteInfo, common::ValueVector* outputVector,
        common::sel_t posInVector);

private:
    std::unique_ptr<LocalVectorCollection> adjChunk;
    std::unique_ptr<RelNGInfo> relNGInfo;
};

class LocalRelTableData final : public LocalTableData {
    friend class RelTableData;

public:
    LocalRelTableData(std::vector<common::LogicalType*> dataTypes, MemoryManager* mm,
        common::ColumnDataFormat dataFormat)
        : LocalTableData{dataTypes, mm, dataFormat} {}

    bool insert(common::ValueVector* srcNodeIDVector, common::ValueVector* dstNodeIDVector,
        const std::vector<common::ValueVector*>& propertyVectors);
    void update(common::ValueVector* srcNodeIDVector, common::ValueVector* relIDVector,
        common::column_id_t columnID, common::ValueVector* propertyVector);
    bool delete_(common::ValueVector* srcNodeIDVector, common::ValueVector* dstNodeIDVector,
        common::ValueVector* relIDVector);

private:
    LocalNodeGroup* getOrCreateLocalNodeGroup(common::ValueVector* nodeIDVector);
};

} // namespace storage
} // namespace kuzu