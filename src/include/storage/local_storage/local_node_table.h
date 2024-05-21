#pragma once

#include "common/copy_constructors.h"
#include "local_table.h"

namespace kuzu {
namespace storage {

class ChunkedNodeGroup;
struct TableScanState;

class LocalNodeNG final : public LocalNodeGroup {
public:
    LocalNodeNG(common::table_id_t tableID, common::offset_t nodeGroupStartOffset,
        const std::vector<common::LogicalType>& dataTypes)
        : LocalNodeGroup{nodeGroupStartOffset, dataTypes}, tableID{tableID} {}
    DELETE_COPY_DEFAULT_MOVE(LocalNodeNG);

    void initializeScanState(TableScanState& scanState) const;
    void scan(TableScanState& scanState);
    void lookup(const common::ValueVector& nodeIDVector,
        const std::vector<common::column_id_t>& columnIDs,
        const std::vector<common::ValueVector*>& outputVectors);

    bool insert(std::vector<common::ValueVector*> nodeIDVectors,
        std::vector<common::ValueVector*> propertyVectors) override;
    bool update(std::vector<common::ValueVector*> nodeIDVectors, common::column_id_t columnID,
        common::ValueVector* propertyVector) override;
    bool delete_(common::ValueVector* nodeIDVector,
        common::ValueVector* /*extraVector*/ = nullptr) override;

    const offset_to_row_idx_t& getInsertInfoRef() const { return insertChunks.getOffsetToRowIdx(); }
    const offset_to_row_idx_t& getUpdateInfoRef(common::column_id_t columnID) const {
        return getUpdateChunks(columnID).getOffsetToRowIdx();
    }

private:
    common::table_id_t tableID;
};

class LocalNodeTableData final : public LocalTableData {
public:
    explicit LocalNodeTableData(common::table_id_t tableID,
        std::vector<common::LogicalType> dataTypes)
        : LocalTableData{tableID, std::move(dataTypes)} {}

private:
    LocalNodeGroup* getOrCreateLocalNodeGroup(common::ValueVector* nodeIDVector) override;
};

struct TableReadState;
class LocalNodeTable final : public LocalTable {
public:
    explicit LocalNodeTable(Table& table);

    bool insert(TableInsertState& state) override;
    bool update(TableUpdateState& state) override;
    bool delete_(TableDeleteState& deleteState) override;

    LocalNodeTableData* getTableData() const {
        KU_ASSERT(localTableDataCollection.size() == 1);
        return common::ku_dynamic_cast<LocalTableData*, LocalNodeTableData*>(
            localTableDataCollection[0].get());
    }
};

} // namespace storage
} // namespace kuzu
