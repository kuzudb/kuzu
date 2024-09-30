#pragma once

#include "catalog/catalog_entry/rel_table_catalog_entry.h"
#include "storage/store/rel_table_data.h"
#include "storage/store/table.h"

namespace kuzu {
namespace evaluator {
class ExpressionEvaluator;
} // namespace evaluator
namespace transaction {
class Transaction;
}
namespace storage {
class MemoryManager;

struct LocalRelTableScanState;
struct RelTableScanState : TableScanState {
    common::RelDataDirection direction;
    common::offset_t boundNodeOffset;
    Column* csrOffsetColumn;
    Column* csrLengthColumn;

    std::unique_ptr<LocalRelTableScanState> localTableScanState;

    // Scan state for un-committed data.
    RelTableScanState(common::table_id_t tableID, const std::vector<common::column_id_t>& columnIDs)
        : RelTableScanState{tableID, columnIDs, {}, nullptr /*csrOffsetCol*/,
              nullptr /*csrLengthCol*/,
              common::RelDataDirection::FWD /* This is a dummy direction */} {}

    RelTableScanState(common::table_id_t tableID, const std::vector<common::column_id_t>& columnIDs,
        const std::vector<Column*>& columns, Column* csrOffsetCol, Column* csrLengthCol,
        common::RelDataDirection direction)
        : RelTableScanState(tableID, columnIDs, columns, csrOffsetCol, csrLengthCol, direction,
              std::vector<ColumnPredicateSet>{}) {}
    RelTableScanState(common::table_id_t tableID, const std::vector<common::column_id_t>& columnIDs,
        const std::vector<Column*>& columns, Column* csrOffsetCol, Column* csrLengthCol,
        common::RelDataDirection direction, std::vector<ColumnPredicateSet> columnPredicateSets);

    void initState(transaction::Transaction* transaction, NodeGroup* nodeGroup) override;

    bool scanNext(transaction::Transaction* transaction) override;

    void resetState() override {
        boundNodeOffset = common::INVALID_OFFSET;
        nodeGroupScanState->resetState();
    }

private:
    void initLocalState() const;
};

class LocalRelTable;
struct LocalRelTableScanState final : RelTableScanState {
    LocalRelTable* localRelTable;
    // TODO(Guodong): Copy of rowIndices here is only to simplify the implementation. We can always
    // go to the fwdIndex/bwdIndex inside LocalRelTable to find the row indices. We can revisit this
    // if the copy of rowIndices from fwdIndex/bwdIndex becomes a problem.
    row_idx_vec_t rowIndices;
    common::row_idx_t nextRowToScan = 0;

    // TODO(Guodong): Remove duplicated fields here by keep a reference to the original state.
    LocalRelTableScanState(const RelTableScanState& state,
        const std::vector<common::column_id_t>& columnIDs, LocalRelTable* localRelTable)
        : RelTableScanState{state.tableID, columnIDs}, localRelTable{localRelTable} {
        direction = state.direction;
        nodeIDVector = state.nodeIDVector;
        outputVectors = state.outputVectors;
        // Setting source to UNCOMMITTED is not necessary but just to keep it semantically
        // consistent.
        source = TableScanSource::UNCOMMITTED;
    }
};

struct RelTableInsertState final : TableInsertState {
    common::ValueVector& srcNodeIDVector;
    common::ValueVector& dstNodeIDVector;

    RelTableInsertState(common::ValueVector& srcNodeIDVector, common::ValueVector& dstNodeIDVector,
        std::vector<common::ValueVector*>& propertyVectors)
        : TableInsertState{std::move(propertyVectors)}, srcNodeIDVector{srcNodeIDVector},
          dstNodeIDVector{dstNodeIDVector} {}
};

struct RelTableUpdateState final : TableUpdateState {
    common::ValueVector& srcNodeIDVector;
    common::ValueVector& dstNodeIDVector;
    common::ValueVector& relIDVector;

    RelTableUpdateState(common::column_id_t columnID, common::ValueVector& srcNodeIDVector,
        common::ValueVector& dstNodeIDVector, common::ValueVector& relIDVector,
        common::ValueVector& propertyVector)
        : TableUpdateState{columnID, propertyVector}, srcNodeIDVector{srcNodeIDVector},
          dstNodeIDVector{dstNodeIDVector}, relIDVector{relIDVector} {}
};

struct RelTableDeleteState final : TableDeleteState {
    common::ValueVector& srcNodeIDVector;
    common::ValueVector& dstNodeIDVector;
    common::ValueVector& relIDVector;

    RelTableDeleteState(common::ValueVector& srcNodeIDVector, common::ValueVector& dstNodeIDVector,
        common::ValueVector& relIDVector)
        : srcNodeIDVector{srcNodeIDVector}, dstNodeIDVector{dstNodeIDVector},
          relIDVector{relIDVector} {}
};

class RelTable final : public Table {
public:
    RelTable(catalog::RelTableCatalogEntry* relTableEntry, const StorageManager* storageManager,
        MemoryManager* memoryManager, common::Deserializer* deSer = nullptr);

    static std::unique_ptr<RelTable> loadTable(common::Deserializer& deSer,
        const catalog::Catalog& catalog, StorageManager* storageManager,
        MemoryManager* memoryManager, common::VirtualFileSystem* vfs, main::ClientContext* context);

    common::table_id_t getFromNodeTableID() const { return fromNodeTableID; }
    common::table_id_t getToNodeTableID() const { return toNodeTableID; }

    void initScanState(transaction::Transaction* transaction, TableScanState& scanState) override;

    bool scanInternal(transaction::Transaction* transaction, TableScanState& scanState) override;

    void insert(transaction::Transaction* transaction, TableInsertState& insertState) override;
    void update(transaction::Transaction* transaction, TableUpdateState& updateState) override;
    bool delete_(transaction::Transaction* transaction, TableDeleteState& deleteState) override;

    void detachDelete(transaction::Transaction* transaction, common::RelDataDirection direction,
        RelTableDeleteState* deleteState);
    void checkIfNodeHasRels(transaction::Transaction* transaction,
        common::RelDataDirection direction, common::ValueVector* srcNodeIDVector) const;

    void addColumn(transaction::Transaction* transaction,
        TableAddColumnState& addColumnState) override;
    Column* getCSROffsetColumn(common::RelDataDirection direction) const {
        return direction == common::RelDataDirection::FWD ? fwdRelTableData->getCSROffsetColumn() :
                                                            bwdRelTableData->getCSROffsetColumn();
    }
    Column* getCSRLengthColumn(common::RelDataDirection direction) const {
        return direction == common::RelDataDirection::FWD ? fwdRelTableData->getCSRLengthColumn() :
                                                            bwdRelTableData->getCSRLengthColumn();
    }
    common::column_id_t getNumColumns() const {
        KU_ASSERT(fwdRelTableData->getNumColumns() == bwdRelTableData->getNumColumns());
        return fwdRelTableData->getNumColumns();
    }
    Column* getColumn(common::column_id_t columnID, common::RelDataDirection direction) const {
        return direction == common::RelDataDirection::FWD ? fwdRelTableData->getColumn(columnID) :
                                                            bwdRelTableData->getColumn(columnID);
    }

    NodeGroup* getOrCreateNodeGroup(common::node_group_idx_t nodeGroupIdx,
        common::RelDataDirection direction) const;

    void commit(transaction::Transaction* transaction, LocalTable* localTable) override;
    void checkpoint(common::Serializer& ser, catalog::TableCatalogEntry* tableEntry) override;

    common::row_idx_t getNumRows() override { return nextRelOffset; }

    RelTableData* getDirectedTableData(common::RelDataDirection direction) const {
        return direction == common::RelDataDirection::FWD ? fwdRelTableData.get() :
                                                            bwdRelTableData.get();
    }

    common::offset_t reserveRelOffsets(common::offset_t numRels) {
        std::unique_lock xLck{relOffsetMtx};
        const auto currentRelOffset = nextRelOffset;
        nextRelOffset += numRels;
        return currentRelOffset;
    }

private:
    static void prepareCommitForNodeGroup(const transaction::Transaction* transaction,
        NodeGroup& localNodeGroup, CSRNodeGroup& csrNodeGroup, common::offset_t boundOffsetInGroup,
        const row_idx_vec_t& rowIndices, common::column_id_t skippedColumn);

    void updateRelOffsets(const LocalRelTable& localRelTable);
    static void updateNodeOffsets(const transaction::Transaction* transaction,
        LocalRelTable& localRelTable);

    static common::offset_t getCommittedOffset(common::offset_t uncommittedOffset,
        common::offset_t maxCommittedOffset);

    void detachDeleteForCSRRels(transaction::Transaction* transaction,
        const RelTableData* tableData, const RelTableData* reverseTableData,
        RelTableScanState* relDataReadState, RelTableDeleteState* deleteState);

private:
    common::table_id_t fromNodeTableID;
    common::table_id_t toNodeTableID;
    std::mutex relOffsetMtx;
    common::offset_t nextRelOffset;
    std::unique_ptr<RelTableData> fwdRelTableData;
    std::unique_ptr<RelTableData> bwdRelTableData;
};

} // namespace storage
} // namespace kuzu
