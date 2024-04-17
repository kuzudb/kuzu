#pragma once

#include "storage/store/table_data.h"

namespace kuzu {
namespace storage {

class LocalNodeNG;
struct NodeDataReadState : public TableDataReadState {
    NodeDataReadState() : TableDataReadState{} {}
    DELETE_COPY_DEFAULT_MOVE(NodeDataReadState);

    common::node_group_idx_t nodeGroupIdx = common::INVALID_NODE_GROUP_IDX;
    bool readFromPersistent = false;
    std::vector<Column::ReadState> columnReadStates;

    // Should be moved into LocalNodeReadState.
    LocalNodeNG* localNodeGroup = nullptr;
};

class LocalTableData;
class NodeTableData final : public TableData {
public:
    NodeTableData(BMFileHandle* dataFH, BMFileHandle* metadataFH,
        catalog::TableCatalogEntry* tableEntry, BufferManager* bufferManager, WAL* wal,
        const std::vector<catalog::Property>& properties, TablesStatistics* tablesStatistics,
        bool enableCompression);

    // This interface is node table specific, as rel table requires also relDataDirection.
    virtual void initializeReadState(transaction::Transaction* transaction,
        std::vector<common::column_id_t> columnIDs, const common::ValueVector& inNodeIDVector,
        TableDataReadState& readState);
    void read(transaction::Transaction* transaction, TableDataReadState& readState,
        const common::ValueVector& nodeIDVector,
        const std::vector<common::ValueVector*>& outputVectors);

    // Flush the nodeGroup to disk and update metadataDAs.
    void append(ChunkedNodeGroup* nodeGroup) override;

    void prepareLocalTableToCommit(transaction::Transaction* transaction,
        LocalTableData* localTable) override;

    common::node_group_idx_t getNumNodeGroups(
        transaction::Transaction* transaction) const override {
        return columns[0]->getNumNodeGroups(transaction);
    }

private:
    void initializeLocalNodeReadState(transaction::Transaction* transaction,
        NodeDataReadState& readState, common::node_group_idx_t nodeGroupIdx);

    void scan(transaction::Transaction* transaction, TableDataReadState& readState,
        const common::ValueVector& nodeIDVector,
        const std::vector<common::ValueVector*>& outputVectors) override;
    void lookup(transaction::Transaction* transaction, TableDataReadState& readState,
        const common::ValueVector& nodeIDVector,
        const std::vector<common::ValueVector*>& outputVectors) override;

    void append(transaction::Transaction* transaction, common::node_group_idx_t nodeGroupIdx,
        LocalNodeGroup* localNodeGroup);
    void merge(transaction::Transaction* transaction, common::node_group_idx_t nodeGroupIdx,
        LocalNodeGroup* nodeGroup);
};

} // namespace storage
} // namespace kuzu
