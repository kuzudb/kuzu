#pragma once

#include "storage/store/rel_table_data.h"

namespace kuzu {
namespace storage {

struct CSRHeaderColumns {
    std::unique_ptr<Column> offset;
    std::unique_ptr<Column> length;
};

class CSRRelTableData final : public RelTableData {
public:
    CSRRelTableData(BMFileHandle* dataFH, BMFileHandle* metadataFH, BufferManager* bufferManager,
        WAL* wal, catalog::RelTableSchema* tableSchema, RelsStoreStats* relsStoreStats,
        common::RelDataDirection direction, bool enableCompression);

    void initializeReadState(transaction::Transaction* transaction,
        std::vector<common::column_id_t> columnIDs, common::ValueVector* inNodeIDVector,
        RelDataReadState* readState) override;
    void scan(transaction::Transaction* transaction, TableReadState& readState,
        common::ValueVector* inNodeIDVector,
        const std::vector<common::ValueVector*>& outputVectors) override;

    bool checkIfNodeHasRels(
        transaction::Transaction* transaction, common::ValueVector* srcNodeIDVector) override;
    void append(NodeGroup* nodeGroup) override;
    void resizeColumns(common::node_group_idx_t numNodeGroups) override;

    inline Column* getCSROffsetColumn() const override { return csrHeaderColumns.offset.get(); }
    inline Column* getCSRLengthColumn() const override { return csrHeaderColumns.length.get(); }

    void prepareLocalTableToCommit(
        transaction::Transaction* transaction, LocalTableData* localTable) override;

    void checkpointInMemory() override;
    void rollbackInMemory() override;

private:
    void prepareCommitCSRNGWithoutSliding(transaction::Transaction* transaction,
        common::node_group_idx_t nodeGroupIdx, CSRRelNGInfo* relNodeGroupInfo,
        ColumnChunk* csrOffsetChunk, ColumnChunk* csrLengthChunk, ColumnChunk* relIDChunk,
        LocalRelNG* localNodeGroup);
    void prepareCommitCSRNGWithSliding(transaction::Transaction* transaction,
        common::node_group_idx_t nodeGroupIdx, CSRRelNGInfo* relNodeGroupInfo,
        ColumnChunk* csrOffsetChunk, ColumnChunk* csrLengthChunk, ColumnChunk* relIDChunk,
        LocalRelNG* localNodeGroup);

    std::pair<std::unique_ptr<ColumnChunk>, std::unique_ptr<ColumnChunk>> slideCSRAuxChunks(
        ColumnChunk* csrOffsetChunk, ColumnChunk* csrLengthChunk, CSRRelNGInfo* relNodeGroupInfo);
    std::unique_ptr<ColumnChunk> slideCSRColumnChunk(transaction::Transaction* transaction,
        ColumnChunk* csrOffsetChunk, ColumnChunk* csrLengthChunk,
        ColumnChunk* slidedCSROffsetChunkForCheck, ColumnChunk* relIDChunk,
        const offset_to_offset_to_row_idx_t& insertInfo,
        const offset_to_offset_to_row_idx_t& updateInfo, const offset_to_offset_set_t& deleteInfo,
        common::node_group_idx_t nodeGroupIdx, Column* column, LocalVectorCollection* localChunk);

private:
    CSRHeaderColumns csrHeaderColumns;
};

} // namespace storage
} // namespace kuzu
