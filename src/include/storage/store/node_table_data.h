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

    common::vector_idx_t vectorIdx = 0;
    common::node_group_idx_t nodeGroupIdx = common::INVALID_NODE_GROUP_IDX;
    common::row_idx_t numRows = 0;

    bool readFromPersistent = false;
    std::vector<Column::ChunkState> chunkReadStates;
    LocalNodeNG* localNodeGroup = nullptr;

    bool hasMoreToRead(const transaction::Transaction*) override {
        if (vectorIdx * common::DEFAULT_VECTOR_CAPACITY >= numRows) {
            return false;
        }
        return vectorIdx * common::DEFAULT_VECTOR_CAPACITY < numRows;
    }
};

class LocalTableData;
class NodeTableData final : public TableData {
public:
    NodeTableData(BMFileHandle* dataFH, BMFileHandle* metadataFH,
        catalog::TableCatalogEntry* tableEntry, BufferManager* bufferManager, WAL* wal,
        const std::vector<catalog::Property>& properties, TablesStatistics* tablesStatistics,
        bool enableCompression);

    // This interface is node table specific, as rel table requires also relDataDirection.
    void initializeReadState(transaction::Transaction* transaction,
        common::node_group_idx_t nodeGroupIdx, const std::vector<common::column_id_t>& columnIDs,
        TableDataReadState& readState) const;
    void read(transaction::Transaction* transaction, TableDataReadState& readState,
        common::ValueVector& nodeIDVector, const std::vector<common::ValueVector*>& outputVectors);

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
    void initializeColumnReadStates(transaction::Transaction* transaction,
        NodeDataReadState& readState, common::node_group_idx_t nodeGroupIdx) const;
    void initializeLocalNodeReadState(transaction::Transaction* transaction,
        NodeDataReadState& readState, common::node_group_idx_t nodeGroupIdx) const;

    void scan(transaction::Transaction* transaction, TableDataReadState& readState,
        common::ValueVector& nodeIDVector,
        const std::vector<common::ValueVector*>& outputVectors) override;
    void lookup(transaction::Transaction* transaction, TableDataReadState& readState,
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
