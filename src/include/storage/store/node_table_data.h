#pragma once

#include "common/types/internal_id_t.h"
#include "common/types/types.h"
#include "storage/store/table_data.h"

namespace kuzu {
namespace storage {
class TablesStatistics;

class LocalNodeNG;
struct NodeDataReadState : TableDataReadState {
    NodeDataReadState() : TableDataReadState{} {}
    DELETE_COPY_DEFAULT_MOVE(NodeDataReadState);

    common::row_idx_t numRowsToScan = 0;
    common::vector_idx_t vectorIdx = common::INVALID_VECTOR_IDX;
    common::node_group_idx_t nodeGroupIdx = common::INVALID_NODE_GROUP_IDX;
    common::row_idx_t numRowsInNodeGroup = 0;

    bool readFromPersistent = false;
    std::vector<Column::ChunkState> chunkReadStates;
    LocalNodeNG* localNodeGroup = nullptr;

    bool hasMoreToRead(const transaction::Transaction*) override;
    void nextVector();
};

class LocalTableData;
class NodeTableData final : public TableData {
public:
    NodeTableData(BMFileHandle* dataFH, BMFileHandle* metadataFH,
        catalog::TableCatalogEntry* tableEntry, BufferManager* bufferManager, WAL* wal,
        const std::vector<catalog::Property>& properties, TablesStatistics* tablesStatistics,
        bool enableCompression);

    // This interface is node table specific, as rel table requires also relDataDirection.
    void initializeScanState(transaction::Transaction* transaction,
        common::node_group_idx_t nodeGroupIdx, const std::vector<common::column_id_t>& columnIDs,
        TableDataReadState& readState) const;
    void scan(transaction::Transaction* transaction, TableDataReadState& readState,
        common::ValueVector& nodeIDVector,
        const std::vector<common::ValueVector*>& outputVectors) override;

    // Flush the nodeGroup to disk and update metadataDAs.
    void append(transaction::Transaction* transaction, ChunkedNodeGroup* nodeGroup) override;

    void prepareLocalNodeGroupToCommit(common::node_group_idx_t nodeGroupIdx,
        transaction::Transaction* transaction, LocalNodeNG* localNodeGroup) const;
    void prepareLocalTableToCommit(transaction::Transaction* transaction,
        LocalTableData* localTable) override;

    common::node_group_idx_t getNumNodeGroups(
        const transaction::Transaction* transaction) const override {
        return columns[0]->getNumNodeGroups(transaction);
    }

    common::offset_t getNumTuplesInNodeGroup(const transaction::Transaction* transaction,
        common::node_group_idx_t nodeGroupIdx) const {
        KU_ASSERT(nodeGroupIdx < getNumNodeGroups(transaction));
        return columns[0]->getMetadata(nodeGroupIdx, transaction->getType()).numValues;
    }

private:
    void initializeColumnScanStates(transaction::Transaction* transaction,
        NodeDataReadState& readState, common::node_group_idx_t nodeGroupIdx) const;
    void initializeLocalNodeReadState(transaction::Transaction* transaction,
        NodeDataReadState& readState, common::node_group_idx_t nodeGroupIdx) const;

    void lookup(transaction::Transaction* transaction, TableDataReadState& state,
        const common::ValueVector& nodeIDVector,
        const std::vector<common::ValueVector*>& outputVectors) override;

    void append(transaction::Transaction* transaction, common::node_group_idx_t nodeGroupIdx,
        LocalNodeGroup* localNodeGroup);
    void merge(transaction::Transaction* transaction, common::node_group_idx_t nodeGroupIdx,
        LocalNodeGroup* nodeGroup);

    static bool sanityCheckOnColumnNumValues(const NodeDataReadState& readState);
};

} // namespace storage
} // namespace kuzu
