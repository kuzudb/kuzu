#pragma once

#include <utility>

#include "common/copy_constructors.h"
#include "local_table.h"

namespace kuzu {
namespace storage {

class ChunkedNodeGroup;

class LocalNodeNG final : public LocalNodeGroup {
public:
    LocalNodeNG(common::offset_t nodeGroupStartOffset,
        const std::vector<common::LogicalType>& dataTypes)
        : LocalNodeGroup{nodeGroupStartOffset, dataTypes} {}
    DELETE_COPY_DEFAULT_MOVE(LocalNodeNG);

    void scan(const common::ValueVector& nodeIDVector,
        const std::vector<common::column_id_t>& columnIDs,
        const std::vector<common::ValueVector*>& outputVectors);
    void lookup(const common::ValueVector& nodeIDVector, common::offset_t offsetInVectorToLookup,
        const std::vector<common::column_id_t>& columnIDs,
        const std::vector<common::ValueVector*>& outputVectors);

    bool insert(std::vector<common::ValueVector*> nodeIDVectors,
        std::vector<common::ValueVector*> propertyVectors) override;
    bool insert(common::offset_t startNodeOffset, ChunkedNodeGroup* nodeGroup,
        common::offset_t numValues);
    bool update(std::vector<common::ValueVector*> nodeIDVectors, common::column_id_t columnID,
        common::ValueVector* propertyVector) override;
    bool delete_(common::ValueVector* nodeIDVector,
        common::ValueVector* /*extraVector*/ = nullptr) override;

    inline const offset_to_row_idx_t& getInsertInfoRef() const {
        return insertChunks.getOffsetToRowIdx();
    }
    inline const offset_to_row_idx_t& getUpdateInfoRef(common::column_id_t columnID) const {
        return getUpdateChunks(columnID).getOffsetToRowIdx();
    }
};

class LocalNodeTableData final : public LocalTableData {
public:
    explicit LocalNodeTableData(std::vector<common::LogicalType> dataTypes)
        : LocalTableData{std::move(dataTypes)} {}

private:
    LocalNodeGroup* getOrCreateLocalNodeGroup(common::ValueVector* nodeIDVector) override;
};

struct TableReadState;
class LocalNodeTable final : public LocalTable {
public:
    explicit LocalNodeTable(Table& table);

    bool insert(TableInsertState& insertState) override;
    bool update(TableUpdateState& updateState) override;
    bool delete_(TableDeleteState& deleteState) override;

    LocalNodeTableData* getTableData() {
        KU_ASSERT(localTableDataCollection.size() == 1);
        return common::ku_dynamic_cast<LocalTableData*, LocalNodeTableData*>(
            localTableDataCollection[0].get());
    }
};

} // namespace storage
} // namespace kuzu
