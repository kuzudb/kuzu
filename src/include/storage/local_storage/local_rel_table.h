#pragma once

#include "common/enums/rel_multiplicity.h"
#include "common/vector/value_vector.h"
#include "storage/local_storage/local_table.h"

namespace kuzu {
namespace storage {

static constexpr common::column_id_t REL_ID_COLUMN_ID = 0;

// Info of node groups with CSR chunks for rel tables.
// Note that srcNodeOffset here are the relative offset within each node group.
struct RelNGInfo {
    update_insert_info_t adjInsertInfo;
    std::vector<update_insert_info_t> insertInfoPerChunk;
    std::vector<update_insert_info_t> updateInfoPerChunk;
    delete_info_t deleteInfo;
    common::RelMultiplicity multiplicity;

    RelNGInfo(common::RelMultiplicity multiplicity, common::column_id_t numChunks)
        : multiplicity{multiplicity} {
        insertInfoPerChunk.resize(numChunks);
        updateInfoPerChunk.resize(numChunks);
    }

    bool insert(common::offset_t srcOffsetInChunk, common::offset_t relOffset,
        common::row_idx_t adjNodeRowIdx, const std::vector<common::row_idx_t>& propertyNodesRowIdx);
    void update(common::offset_t srcOffsetInChunk, common::offset_t relOffset,
        common::column_id_t columnID, common::row_idx_t rowIdx);
    bool delete_(common::offset_t srcOffsetInChunk, common::offset_t relOffset);

    bool hasUpdates();

    uint64_t getNumInsertedTuples(common::offset_t srcOffsetInChunk);

    const update_insert_info_t& getUpdateInfo(common::column_id_t columnID) {
        KU_ASSERT(columnID == common::INVALID_COLUMN_ID || columnID < updateInfoPerChunk.size());
        return columnID == common::INVALID_COLUMN_ID ? getEmptyInfo() :
                                                       updateInfoPerChunk[columnID];
    }
    const update_insert_info_t& getInsertInfo(common::column_id_t columnID) {
        KU_ASSERT(columnID == common::INVALID_COLUMN_ID || columnID < insertInfoPerChunk.size());
        return columnID == common::INVALID_COLUMN_ID ? adjInsertInfo : insertInfoPerChunk[columnID];
    }
    const delete_info_t& getDeleteInfo() const { return deleteInfo; }

    const update_insert_info_t& getEmptyInfo();

private:
    inline static bool contains(
        const std::unordered_set<common::offset_t>& set, common::offset_t value) {
        return set.find(value) != set.end();
    }
};

class LocalRelNG final : public LocalNodeGroup {
public:
    LocalRelNG(common::offset_t nodeGroupStartOffset, std::vector<common::LogicalType*> dataTypes,
        MemoryManager* mm, common::RelMultiplicity multiplicity);

    common::row_idx_t scanCSR(common::offset_t srcOffsetInChunk,
        common::offset_t posToReadForOffset, const std::vector<common::column_id_t>& columnIDs,
        const std::vector<common::ValueVector*>& outputVector);
    // For CSR, we need to apply updates and deletions here, while insertions are handled by
    // `scanCSR`.
    void applyLocalChangesForCSRColumns(common::offset_t srcOffsetInChunk,
        const std::vector<common::column_id_t>& columnIDs, common::ValueVector* relIDVector,
        const std::vector<common::ValueVector*>& outputVectors);

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
        common::ValueVector* relIDVector, common::ValueVector* outputVector);
    void applyCSRDeletions(common::offset_t srcOffsetInChunk, const delete_info_t& deleteInfo,
        common::ValueVector* relIDVector);

private:
    std::unique_ptr<LocalVectorCollection> adjChunk;
    std::unique_ptr<RelNGInfo> relNGInfo;
};

class LocalRelTableData final : public LocalTableData {
    friend class RelTableData;

public:
    LocalRelTableData(common::RelMultiplicity multiplicity,
        std::vector<common::LogicalType*> dataTypes, MemoryManager* mm)
        : LocalTableData{std::move(dataTypes), mm}, multiplicity{multiplicity} {}

    bool insert(common::ValueVector* srcNodeIDVector, common::ValueVector* dstNodeIDVector,
        const std::vector<common::ValueVector*>& propertyVectors);
    void update(common::ValueVector* srcNodeIDVector, common::ValueVector* relIDVector,
        common::column_id_t columnID, common::ValueVector* propertyVector);
    bool delete_(common::ValueVector* srcNodeIDVector, common::ValueVector* dstNodeIDVector,
        common::ValueVector* relIDVector);

private:
    LocalNodeGroup* getOrCreateLocalNodeGroup(common::ValueVector* nodeIDVector) override;

private:
    common::RelMultiplicity multiplicity;
};

} // namespace storage
} // namespace kuzu
