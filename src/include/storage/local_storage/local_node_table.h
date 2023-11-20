#pragma once

#include "local_table.h"

namespace kuzu {
namespace storage {

class LocalNodeNG final : public LocalNodeGroup {
public:
    LocalNodeNG(common::offset_t nodeGroupStartOffset, std::vector<common::LogicalType*> dataTypes,
        MemoryManager* mm)
        : LocalNodeGroup{nodeGroupStartOffset, dataTypes, mm} {
        insertInfo.resize(dataTypes.size());
        updateInfo.resize(dataTypes.size());
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

    common::row_idx_t getRowIdx(common::column_id_t columnID, common::offset_t nodeOffset);

    inline const offset_to_row_idx_t& getInsertInfoRef(common::column_id_t columnID) {
        KU_ASSERT(columnID < insertInfo.size());
        return insertInfo[columnID];
    }
    inline const offset_to_row_idx_t& getUpdateInfoRef(common::column_id_t columnID) {
        KU_ASSERT(columnID < updateInfo.size());
        return updateInfo[columnID];
    }

private:
    std::vector<offset_to_row_idx_t> insertInfo;
    std::vector<offset_to_row_idx_t> updateInfo;
};

class LocalNodeTableData final : public LocalTableData {
public:
    LocalNodeTableData(std::vector<common::LogicalType*> dataTypes, MemoryManager* mm,
        common::ColumnDataFormat dataFormat)
        : LocalTableData{dataTypes, mm, dataFormat} {}

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

private:
    LocalNodeGroup* getOrCreateLocalNodeGroup(common::ValueVector* nodeIDVector);
};

} // namespace storage
} // namespace kuzu
