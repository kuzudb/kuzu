#pragma once

#include "common/types/internal_id_t.h"
#include "common/types/types.h"
#include "storage/store/table_data.h"

namespace kuzu {
namespace storage {
class TablesStatistics;

class LocalNodeTable;
class NodeTableData final : public TableData {
public:
    NodeTableData(BMFileHandle* dataFH, DiskArrayCollection* metadataDAC,
        catalog::TableCatalogEntry* tableEntry, BufferManager* bufferManager, WAL* wal,
        const std::vector<catalog::Property>& properties, TablesStatistics* tablesStatistics,
        bool enableCompression);

    // This interface is node table specific, as rel table requires also relDataDirection.
    void initializeScanState(transaction::Transaction* transaction,
        TableScanState& scanState) const override;
    void scan(transaction::Transaction* transaction, TableDataScanState& scanState,
        common::ValueVector& nodeIDVector,
        const std::vector<common::ValueVector*>& outputVectors) override;

    // Flush the nodeGroup to disk and update metadataDAs.
    common::offset_t append(transaction::Transaction* transaction,
        ChunkedNodeGroup* nodeGroup) override;

    common::node_group_idx_t getNumCommittedNodeGroups() const override {
        return columns[0]->getNumCommittedNodeGroups();
    }
    std::unique_ptr<ChunkedNodeGroup> getCommittedNodeGroup(
        common::node_group_idx_t nodeGroupIdx) const override;

    void lookup(transaction::Transaction* transaction, TableDataScanState& state,
        const common::ValueVector& nodeIDVector,
        const std::vector<common::ValueVector*>& outputVectors) override;

private:
    void initializeColumnScanStates(transaction::Transaction* transaction,
        TableScanState& scanState, common::node_group_idx_t nodeGroupIdx) const;
    void initializeLocalNodeReadState(transaction::Transaction* transaction,
        TableScanState& scanState, common::node_group_idx_t nodeGroupIdx) const;
};

} // namespace storage
} // namespace kuzu
