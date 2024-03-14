#pragma once

#include <utility>

#include "common/copy_constructors.h"
#include "local_table.h"

namespace kuzu {
namespace storage {

class LocalNodeNG final : public LocalNodeGroup {
public:
    LocalNodeNG(
        common::offset_t nodeGroupStartOffset, const std::vector<common::LogicalType>& dataTypes)
        : LocalNodeGroup{nodeGroupStartOffset, dataTypes} {}
    DELETE_COPY_DEFAULT_MOVE(LocalNodeNG);

    void scan(common::ValueVector* nodeIDVector, const std::vector<common::column_id_t>& columnIDs,
        const std::vector<common::ValueVector*>& outputVectors);
    void lookup(common::offset_t nodeOffset, common::column_id_t columnID,
        common::ValueVector* outputVector, common::sel_t posInOutputVector);

    bool insert(std::vector<common::ValueVector*> nodeIDVectors,
        std::vector<common::ValueVector*> propertyVectors) override;
    bool update(std::vector<common::ValueVector*> nodeIDVectors, common::column_id_t columnID,
        common::ValueVector* propertyVector) override;
    bool delete_(
        common::ValueVector* nodeIDVector, common::ValueVector* /*extraVector*/ = nullptr) override;

    inline const offset_to_row_idx_t& getInsertInfoRef() {
        return insertChunks.getOffsetToRowIdx();
    }
    inline const offset_to_row_idx_t& getUpdateInfoRef(common::column_id_t columnID) {
        return getUpdateChunks(columnID).getOffsetToRowIdx();
    }
};

class LocalNodeTableData final : public LocalTableData {
public:
    explicit LocalNodeTableData(std::vector<common::LogicalType> dataTypes)
        : LocalTableData{std::move(dataTypes)} {}

    void scan(common::ValueVector* nodeIDVector, const std::vector<common::column_id_t>& columnIDs,
        const std::vector<common::ValueVector*>& outputVectors);
    void lookup(common::ValueVector* nodeIDVector,
        const std::vector<common::column_id_t>& columnIDs,
        const std::vector<common::ValueVector*>& outputVectors);

private:
    LocalNodeGroup* getOrCreateLocalNodeGroup(common::ValueVector* nodeIDVector) override;
};

} // namespace storage
} // namespace kuzu
