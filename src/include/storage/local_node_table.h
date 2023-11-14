#pragma once

#include "storage/local_table.h"

namespace kuzu {
namespace storage {

class LocalNodeNG final : public LocalNodeGroup {
    friend class NodeTableData;

public:
    LocalNodeNG(
        const std::vector<std::unique_ptr<common::LogicalType>>& dataTypes, MemoryManager* mm)
        : LocalNodeGroup(dataTypes, mm) {
        updateInfoPerColumn.resize(dataTypes.size());
        insertInfoPerColumn.resize(dataTypes.size());
    }

    void scan(common::ValueVector* nodeIDVector, const std::vector<common::column_id_t>& columnIDs,
        const std::vector<common::ValueVector*>& outputVectors);
    void lookup(common::offset_t nodeOffset, common::column_id_t columnID,
        common::ValueVector* outputVector, common::sel_t posInOutputVector);
    void insert(common::ValueVector* nodeIDVector,
        const std::vector<common::ValueVector*>& propertyVectors);
    void update(common::ValueVector* nodeIDVector, common::column_id_t columnID,
        common::ValueVector* propertyVector);
    void delete_(common::ValueVector* nodeIDVector);

    std::map<common::offset_t, common::row_idx_t>& getUpdateInfoRef(common::column_id_t columnID) {
        return updateInfoPerColumn[columnID];
    }
    std::map<common::offset_t, common::row_idx_t>& getInsertInfoRef(common::column_id_t columnID) {
        return insertInfoPerColumn[columnID];
    }

    inline common::row_idx_t getRowIdx(common::column_id_t columnID, common::offset_t nodeOffset) {
        KU_ASSERT(columnID < columns.size());
        common::row_idx_t rowIdx = common::INVALID_ROW_IDX;
        if (updateInfoPerColumn[columnID].contains(nodeOffset)) {
            // This node is in persistent storage, and had been updated.
            rowIdx = updateInfoPerColumn[columnID].at(nodeOffset);
        } else if (insertInfoPerColumn[columnID].contains(nodeOffset)) {
            // This node is in local storage, and had been newly inserted.
            rowIdx = insertInfoPerColumn[columnID].at(nodeOffset);
        }
        return rowIdx;
    }

private:
    // TODO: Do we need to differentiate between insert and update?
    std::vector<offset_to_row_idx_t> updateInfoPerColumn;
    std::vector<offset_to_row_idx_t> insertInfoPerColumn;
};

class LocalNodeTable final : public LocalTable {
    friend class NodeTableData;

public:
    LocalNodeTable(common::table_id_t tableID,
        std::vector<std::unique_ptr<common::LogicalType>> dataTypes, MemoryManager* mm)
        : LocalTable{tableID, std::move(dataTypes), mm} {}

    void scan(common::ValueVector* nodeIDVector, const std::vector<common::column_id_t>& columnIDs,
        const std::vector<common::ValueVector*>& outputVectors);
    void lookup(common::ValueVector* nodeIDVector,
        const std::vector<common::column_id_t>& columnIDs,
        const std::vector<common::ValueVector*>& outputVectors);
    void insert(common::ValueVector* nodeIDVector,
        const std::vector<common::ValueVector*>& propertyVectors);
    void update(common::ValueVector* nodeIDVector, common::column_id_t columnID,
        common::ValueVector* propertyVector);
    void delete_(common::ValueVector* nodeIDVector);

    inline void clear() { nodeGroups.clear(); }

private:
    common::node_group_idx_t initializeLocalNodeGroup(common::ValueVector* nodeIDVector);

private:
    std::unordered_map<common::node_group_idx_t, std::unique_ptr<LocalNodeNG>> nodeGroups;
};

} // namespace storage
} // namespace kuzu
