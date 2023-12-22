#pragma once

#include "catalog/rel_table_schema.h"
#include "storage/store/node_group.h"
#include "storage/store/table_data.h"

namespace kuzu {
namespace storage {

struct CSRHeaderColumns {
    std::unique_ptr<Column> offset;
    std::unique_ptr<Column> length;
};

class LocalRelNG;
struct RelDataReadState : public TableReadState {
    common::RelDataDirection direction;
    common::ColumnDataFormat dataFormat;
    common::offset_t startNodeOffset;
    common::offset_t numNodes;
    common::offset_t currentNodeOffset;
    common::offset_t posInCurrentCSR;
    std::vector<common::list_entry_t> csrListEntries;
    // Temp auxiliary data structure to scan the offset of each CSR node in the offset column chunk.
    CSRHeaderChunks csrHeaderChunks;

    // Following fields are used for local storage.
    bool readFromLocalStorage;
    LocalRelNG* localNodeGroup;

    explicit RelDataReadState(common::ColumnDataFormat dataFormat);
    inline bool isOutOfRange(common::offset_t nodeOffset) const {
        return nodeOffset < startNodeOffset || nodeOffset >= (startNodeOffset + numNodes);
    }
    bool hasMoreToRead(transaction::Transaction* transaction);
    void populateCSRListEntries();
    std::pair<common::offset_t, common::offset_t> getStartAndEndOffset();

    inline bool hasMoreToReadInPersistentStorage() {
        return posInCurrentCSR < csrListEntries[(currentNodeOffset - startNodeOffset)].size;
    }
    bool hasMoreToReadFromLocalStorage() const;
    bool trySwitchToLocalStorage();
};

class RelsStoreStats;
class LocalRelTableData;
struct CSRRelNGInfo;
class RelTableData : public TableData {
public:
    RelTableData(BMFileHandle* dataFH, BMFileHandle* metadataFH, BufferManager* bufferManager,
        WAL* wal, catalog::RelTableSchema* tableSchema, RelsStoreStats* relsStoreStats,
        common::RelDataDirection direction, bool enableCompression,
        common::ColumnDataFormat dataFormat = common::ColumnDataFormat::REGULAR);

    virtual void initializeReadState(transaction::Transaction* transaction,
        std::vector<common::column_id_t> columnIDs, common::ValueVector* inNodeIDVector,
        RelDataReadState* readState);
    void scan(transaction::Transaction* transaction, TableReadState& readState,
        common::ValueVector* inNodeIDVector,
        const std::vector<common::ValueVector*>& outputVectors) override;
    void lookup(transaction::Transaction* transaction, TableReadState& readState,
        common::ValueVector* inNodeIDVector,
        const std::vector<common::ValueVector*>& outputVectors) override;

    void insert(transaction::Transaction* transaction, common::ValueVector* srcNodeIDVector,
        common::ValueVector* dstNodeIDVector,
        const std::vector<common::ValueVector*>& propertyVectors);
    void update(transaction::Transaction* transaction, common::column_id_t columnID,
        common::ValueVector* srcNodeIDVector, common::ValueVector* relIDVector,
        common::ValueVector* propertyVector);

    // Return true if deletion succeeds. Note that we should return num of rels deleted later when
    // we remove the restriction of flatten all tuples.
    bool delete_(transaction::Transaction* transaction, common::ValueVector* srcNodeIDVector,
        common::ValueVector* dstNodeIDVector, common::ValueVector* relIDVector);
    virtual bool checkIfNodeHasRels(
        transaction::Transaction* transaction, common::ValueVector* srcNodeIDVector);
    void append(NodeGroup* nodeGroup) override;
    virtual void resizeColumns(common::node_group_idx_t numNodeGroups);

    inline Column* getAdjColumn() const { return adjColumn.get(); }
    inline common::ColumnDataFormat getDataFormat() const { return dataFormat; }

    void prepareLocalTableToCommit(
        transaction::Transaction* transaction, LocalTableData* localTable) override;
    void checkpointInMemory() override;
    void rollbackInMemory() override;

    inline common::node_group_idx_t getNumNodeGroups(
        transaction::Transaction* transaction) const override {
        return adjColumn->getNumNodeGroups(transaction);
    }

protected:
    LocalRelNG* getLocalNodeGroup(
        transaction::Transaction* transaction, common::node_group_idx_t nodeGroupIdx);

private:
    static inline common::vector_idx_t getDataIdxFromDirection(common::RelDataDirection direction) {
        return direction == common::RelDataDirection::FWD ? 0 : 1;
    }

protected:
    common::RelDataDirection direction;
    std::unique_ptr<Column> adjColumn;
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
