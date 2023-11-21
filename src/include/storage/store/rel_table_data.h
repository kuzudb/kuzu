#pragma once

#include "catalog/rel_table_schema.h"
#include "common/cast.h"
#include "storage/store/table_data.h"

namespace kuzu {
namespace storage {

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
    std::unique_ptr<ColumnChunk> csrOffsetChunk;

    // Following fields are used for local storage.
    bool readFromLocalStorage;
    LocalRelNG* localNodeGroup;

    RelDataReadState(common::ColumnDataFormat dataFormat);
    inline bool isOutOfRange(common::offset_t nodeOffset) {
        return nodeOffset < startNodeOffset || nodeOffset >= (startNodeOffset + numNodes);
    }
    bool hasMoreToRead(transaction::Transaction* transaction);
    void populateCSRListEntries();
    std::pair<common::offset_t, common::offset_t> getStartAndEndOffset();

    inline bool hasMoreToReadInPersistentStorage() {
        return posInCurrentCSR < csrListEntries[(currentNodeOffset - startNodeOffset)].size;
    }
    bool hasMoreToReadFromLocalStorage();
    bool trySwitchToLocalStorage();
};

class RelsStoreStats;
class LocalRelTableData;
struct CSRRelNGInfo;
class RelTableData final : public TableData {
public:
    RelTableData(BMFileHandle* dataFH, BMFileHandle* metadataFH, BufferManager* bufferManager,
        WAL* wal, catalog::RelTableSchema* tableSchema, RelsStoreStats* relsStoreStats,
        common::RelDataDirection direction, bool enableCompression);

    void initializeReadState(transaction::Transaction* transaction,
        std::vector<common::column_id_t> columnIDs, common::ValueVector* inNodeIDVector,
        RelDataReadState* readState);
    inline void scan(transaction::Transaction* transaction, TableReadState& readState,
        common::ValueVector* inNodeIDVector,
        const std::vector<common::ValueVector*>& outputVectors) {
        auto& relReadState = common::ku_dynamic_cast<TableReadState&, RelDataReadState&>(readState);
        dataFormat == common::ColumnDataFormat::REGULAR ?
            scanRegularColumns(transaction, relReadState, inNodeIDVector, outputVectors) :
            scanCSRColumns(transaction, relReadState, inNodeIDVector, outputVectors);
    }
    void lookup(transaction::Transaction* transaction, TableReadState& readState,
        common::ValueVector* inNodeIDVector,
        const std::vector<common::ValueVector*>& outputVectors);

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
    bool checkIfNodeHasRels(
        transaction::Transaction* transaction, common::ValueVector* srcNodeIDVector);
    void append(NodeGroup* nodeGroup);
    void resizeColumns(common::node_group_idx_t nodeGroupIdx);

    inline Column* getAdjColumn() const { return adjColumn.get(); }
    inline common::ColumnDataFormat getDataFormat() const { return dataFormat; }

    void prepareLocalTableToCommit(
        transaction::Transaction* transaction, LocalTableData* localTable);
    void checkpointInMemory();
    void rollbackInMemory();

private:
    LocalRelNG* getLocalNodeGroup(
        transaction::Transaction* transaction, common::node_group_idx_t nodeGroupIdx);

    void scanRegularColumns(transaction::Transaction* transaction, RelDataReadState& readState,
        common::ValueVector* inNodeIDVector,
        const std::vector<common::ValueVector*>& outputVectors);
    void scanCSRColumns(transaction::Transaction* transaction, RelDataReadState& readState,
        common::ValueVector* inNodeIDVector,
        const std::vector<common::ValueVector*>& outputVectors);

    void prepareCommitForRegularColumns(
        transaction::Transaction* transaction, LocalRelTableData* localTableData);
    void prepareCommitForCSRColumns(
        transaction::Transaction* transaction, LocalRelTableData* localTableData);

    void prepareCommitCSRNGWithoutSliding(transaction::Transaction* transaction,
        common::node_group_idx_t nodeGroupIdx, CSRRelNGInfo* relNodeGroupInfo,
        ColumnChunk* csrOffsetChunk, ColumnChunk* relIDChunk, LocalRelNG* localNodeGroup);
    void prepareCommitCSRNGWithSliding(transaction::Transaction* transaction,
        common::node_group_idx_t nodeGroupIdx, CSRRelNGInfo* relNodeGroupInfo,
        ColumnChunk* csrOffsetChunk, ColumnChunk* relIDChunk, LocalRelNG* localNodeGroup);

    std::unique_ptr<ColumnChunk> slideCSROffsetChunk(
        ColumnChunk* csrOffsetChunk, CSRRelNGInfo* relNodeGroupInfo);
    std::unique_ptr<ColumnChunk> slideCSRColumnChunk(transaction::Transaction* transaction,
        ColumnChunk* csrOffsetChunk, ColumnChunk* slidedCSROffsetChunkForCheck,
        ColumnChunk* relIDChunk, const offset_to_offset_to_row_idx_t& insertInfo,
        const offset_to_offset_to_row_idx_t& updateInfo, const offset_to_offset_set_t& deleteInfo,
        common::node_group_idx_t nodeGroupIdx, Column* column, LocalVectorCollection* localChunk);

    static inline common::ColumnDataFormat getDataFormatFromSchema(
        catalog::RelTableSchema* tableSchema, common::RelDataDirection direction) {
        return tableSchema->isSingleMultiplicityInDirection(direction) ?
                   common::ColumnDataFormat::REGULAR :
                   common::ColumnDataFormat::CSR;
    }
    static inline common::vector_idx_t getDataIdxFromDirection(common::RelDataDirection direction) {
        return direction == common::RelDataDirection::FWD ? 0 : 1;
    }

private:
    common::RelDataDirection direction;
    std::unique_ptr<Column> adjColumn;
    std::unique_ptr<Column> csrOffsetColumn;
};

} // namespace storage
} // namespace kuzu
