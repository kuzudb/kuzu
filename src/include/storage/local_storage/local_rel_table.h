#pragma once

#include "common/column_data_format.h"
#include "storage/local_storage/local_table.h"

namespace kuzu {
namespace storage {

static constexpr common::column_id_t REL_ID_COLUMN_ID = 0;

struct RelNGInfo {
    virtual ~RelNGInfo() = default;

    virtual void insert(common::offset_t srcNodeOffset, common::offset_t relOffset,
        common::row_idx_t adjNodeRowIdx,
        const std::vector<common::row_idx_t>& propertyNodesRowIdx) = 0;
    virtual void update(common::offset_t srcNodeOffset, common::offset_t relOffset,
        common::column_id_t columnID, common::row_idx_t rowIdx) = 0;
    virtual bool delete_(common::offset_t srcNodeOffset, common::offset_t relOffset) = 0;

protected:
    inline static bool contains(
        const std::unordered_set<common::offset_t>& set, common::offset_t value) {
        return set.find(value) != set.end();
    }
};

// Info of node groups with regular chunks for rel tables.
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

    void insert(common::offset_t srcNodeOffset, common::offset_t relOffset,
        common::row_idx_t adjNodeRowIdx,
        const std::vector<common::row_idx_t>& propertyNodesRowIdx) override;
    void update(common::offset_t srcNodeOffset, common::offset_t relOffset,
        common::column_id_t columnID, common::row_idx_t rowIdx) override;
    bool delete_(common::offset_t srcNodeOffset, common::offset_t relOffset) final;
};

// Info of node groups with CSR chunks for rel tables.
struct CSRRelNGInfo final : public RelNGInfo {
    offset_to_offset_to_row_idx_t adjInsertInfo;
    std::vector<offset_to_offset_to_row_idx_t> insertInfoPerChunk;
    std::vector<offset_to_offset_to_row_idx_t> updateInfoPerChunk;
    offset_to_offset_set_t deleteInfo;

    CSRRelNGInfo(common::column_id_t numChunks) {
        insertInfoPerChunk.resize(numChunks);
        updateInfoPerChunk.resize(numChunks);
    }

    void insert(common::offset_t srcNodeOffset, common::offset_t relOffset,
        common::row_idx_t adjNodeRowIdx,
        const std::vector<common::row_idx_t>& propertyNodesRowIdx) override;
    void update(common::offset_t srcNodeOffset, common::offset_t relOffset,
        common::column_id_t columnID, common::row_idx_t rowIdx) override;
    bool delete_(common::offset_t srcNodeOffset, common::offset_t relOffset) override;
};

class LocalRelNG final : public LocalNodeGroup {
public:
    LocalRelNG(common::ColumnDataFormat dataFormat, std::vector<common::LogicalType*> dataTypes,
        MemoryManager* mm);

    void insert(common::ValueVector* srcNodeIDVector, common::ValueVector* dstNodeIDVector,
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
    std::unique_ptr<LocalVectorCollection> adjChunk;
    std::unique_ptr<RelNGInfo> relNGInfo;
};

class LocalRelTableData final : public LocalTableData {
    friend class RelTableData;

public:
    LocalRelTableData(std::vector<common::LogicalType*> dataTypes, MemoryManager* mm,
        common::ColumnDataFormat dataFormat)
        : LocalTableData{dataTypes, mm, dataFormat} {}

    void insert(common::ValueVector* srcNodeIDVector, common::ValueVector* dstNodeIDVector,
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