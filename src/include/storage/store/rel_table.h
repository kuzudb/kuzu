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
    common::sel_t currBoundNodeIdx;
    Column* csrOffsetColumn;
    Column* csrLengthColumn;

    // This is a reference of the original selVector of the input boundNodeIDVector.
    common::SelectionVector cachedBoundNodeSelVector;

    std::unique_ptr<LocalRelTableScanState> localTableScanState;

    RelTableScanState(MemoryManager& mm, common::ValueVector* nodeIDVector,
        std::vector<common::ValueVector*> outputVectors,
        std::shared_ptr<common::DataChunkState> outChunkState)
        : TableScanState{nodeIDVector, std::move(outputVectors), std::move(outChunkState)},
          direction{common::RelDataDirection::INVALID}, currBoundNodeIdx{0},
          csrOffsetColumn{nullptr}, csrLengthColumn{nullptr}, localTableScanState{nullptr} {
        nodeGroupScanState = std::make_unique<CSRNodeGroupScanState>(mm, columnIDs.size());
    }

    // This is for local table scan state.
    RelTableScanState(common::ValueVector* nodeIDVector,
        std::vector<common::ValueVector*> outputVectors,
        std::shared_ptr<common::DataChunkState> outChunkState)
        : TableScanState{nodeIDVector, std::move(outputVectors), std::move(outChunkState)},
          direction{common::RelDataDirection::INVALID}, currBoundNodeIdx{0},
          csrOffsetColumn{nullptr}, csrLengthColumn{nullptr}, localTableScanState{nullptr} {
        nodeGroupScanState = std::make_unique<CSRNodeGroupScanState>(columnIDs.size());
    }

    void setToTable(const transaction::Transaction* transaction, Table* table_,
        std::vector<common::column_id_t> columnIDs_,
        std::vector<ColumnPredicateSet> columnPredicateSets_,
        common::RelDataDirection direction_) override;

    void initState(transaction::Transaction* transaction, NodeGroup* nodeGroup,
        bool resetCachedBoundNodeIDs = true) override;

    bool scanNext(transaction::Transaction* transaction) override;

    void setNodeIDVectorToFlat(common::sel_t selPos) const;

private:
    bool hasUnComittedData() const;

    void initCachedBoundNodeIDSelVector();
    void initStateForCommitted(const transaction::Transaction* transaction);
    void initStateForUncommitted();
};

class LocalRelTable;
struct LocalRelTableScanState final : RelTableScanState {
    LocalRelTable* localRelTable;
    // TODO(Guodong): Copy of rowIndices here is only to simplify the implementation. We can always
    // go to the fwdIndex/bwdIndex inside LocalRelTable to find the row indices. We can revisit this
    // if the copy of rowIndices from fwdIndex/bwdIndex becomes a problem.
    row_idx_vec_t rowIndices;
    common::row_idx_t nextRowToScan = 0;

    LocalRelTableScanState(MemoryManager& mm, const RelTableScanState& baseScanState,
        LocalRelTable* localRelTable, std::vector<common::column_id_t> columnIDs)
        : RelTableScanState{mm, baseScanState.nodeIDVector, baseScanState.outputVectors,
              baseScanState.outState},
          localRelTable{localRelTable} {
        this->columnIDs = std::move(columnIDs);
        this->direction = baseScanState.direction;
        // Setting source to UNCOMMITTED is not necessary but just to keep it semantically
        // consistent.
        this->source = TableScanSource::UNCOMMITTED;
    }
};

struct RelTableInsertState final : TableInsertState {
    common::ValueVector& srcNodeIDVector;
    common::ValueVector& dstNodeIDVector;

    common::ValueVector& getBoundNodeIDVector(common::RelDataDirection direction) const {
        return direction == common::RelDataDirection::FWD ? srcNodeIDVector : dstNodeIDVector;
    }

    RelTableInsertState(common::ValueVector& srcNodeIDVector, common::ValueVector& dstNodeIDVector,
        std::vector<common::ValueVector*> propertyVectors)
        : TableInsertState{std::move(propertyVectors)}, srcNodeIDVector{srcNodeIDVector},
          dstNodeIDVector{dstNodeIDVector} {}
};

struct RelTableUpdateState final : TableUpdateState {
    common::ValueVector& srcNodeIDVector;
    common::ValueVector& dstNodeIDVector;
    common::ValueVector& relIDVector;

    common::ValueVector& getBoundNodeIDVector(common::RelDataDirection direction) const {
        return direction == common::RelDataDirection::FWD ? srcNodeIDVector : dstNodeIDVector;
    }

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

    common::ValueVector& getBoundNodeIDVector(common::RelDataDirection direction) const {
        return direction == common::RelDataDirection::FWD ? srcNodeIDVector : dstNodeIDVector;
    }

    RelTableDeleteState(common::ValueVector& srcNodeIDVector, common::ValueVector& dstNodeIDVector,
        common::ValueVector& relIDVector)
        : srcNodeIDVector{srcNodeIDVector}, dstNodeIDVector{dstNodeIDVector},
          relIDVector{relIDVector} {}
};

class RelTable final : public Table {
public:
    using rel_multiplicity_constraint_throw_func_t =
        std::function<void(const std::string&, common::offset_t, common::RelDataDirection)>;

    RelTable(catalog::RelTableCatalogEntry* relTableEntry, const StorageManager* storageManager,
        MemoryManager* memoryManager, common::Deserializer* deSer = nullptr);

    static std::unique_ptr<RelTable> loadTable(common::Deserializer& deSer,
        const catalog::Catalog& catalog, StorageManager* storageManager,
        MemoryManager* memoryManager, common::VirtualFileSystem* vfs, main::ClientContext* context);

    common::table_id_t getFromNodeTableID() const { return fromNodeTableID; }
    common::table_id_t getToNodeTableID() const { return toNodeTableID; }

    void initScanState(transaction::Transaction* transaction, TableScanState& scanState,
        bool resetCachedBoundNodeSelVec = true) const override;

    bool scanInternal(transaction::Transaction* transaction, TableScanState& scanState) override;

    void insert(transaction::Transaction* transaction, TableInsertState& insertState) override;
    void update(transaction::Transaction* transaction, TableUpdateState& updateState) override;
    bool delete_(transaction::Transaction* transaction, TableDeleteState& deleteState) override;

    void detachDelete(transaction::Transaction* transaction, common::RelDataDirection direction,
        RelTableDeleteState* deleteState);
    bool checkIfNodeHasRels(transaction::Transaction* transaction,
        common::RelDataDirection direction, common::ValueVector* srcNodeIDVector) const;
    void throwIfNodeHasRels(transaction::Transaction* transaction,
        common::RelDataDirection direction, common::ValueVector* srcNodeIDVector,
        const rel_multiplicity_constraint_throw_func_t& throwFunc) const;

    void addColumn(transaction::Transaction* transaction,
        TableAddColumnState& addColumnState) override;
    Column* getCSROffsetColumn(common::RelDataDirection direction) const {
        return getDirectedTableData(direction)->getCSROffsetColumn();
    }
    Column* getCSRLengthColumn(common::RelDataDirection direction) const {
        return getDirectedTableData(direction)->getCSRLengthColumn();
    }
    common::column_id_t getNumColumns() const {
        KU_ASSERT(directedRelData.size() >= 1);
        RUNTIME_CHECK(for (const auto& relData
                           : directedRelData) {
            KU_ASSERT(relData->getNumColumns() == directedRelData[0]->getNumColumns());
        });
        return directedRelData[0]->getNumColumns();
    }
    Column* getColumn(common::column_id_t columnID, common::RelDataDirection direction) const {
        return getDirectedTableData(direction)->getColumn(columnID);
    }

    NodeGroup* getOrCreateNodeGroup(const transaction::Transaction* transaction,
        common::node_group_idx_t nodeGroupIdx, common::RelDataDirection direction) const;

    void commit(transaction::Transaction* transaction, catalog::TableCatalogEntry* tableEntry,
        LocalTable* localTable) override;
    void checkpoint(common::Serializer& ser, catalog::TableCatalogEntry* tableEntry) override;
    void rollbackCheckpoint() override {};

    common::row_idx_t getNumTotalRows(const transaction::Transaction* transaction) override;

    RelTableData* getDirectedTableData(common::RelDataDirection direction) const;

    common::offset_t reserveRelOffsets(common::offset_t numRels) {
        std::unique_lock xLck{relOffsetMtx};
        const auto currentRelOffset = nextRelOffset;
        nextRelOffset += numRels;
        return currentRelOffset;
    }

    void pushInsertInfo(const transaction::Transaction* transaction,
        common::RelDataDirection direction, const CSRNodeGroup& nodeGroup,
        common::row_idx_t numRows_, CSRNodeGroupScanSource source) const;

    std::vector<common::RelDataDirection> getStorageDirections() const;

private:
    static void prepareCommitForNodeGroup(const transaction::Transaction* transaction,
        const std::vector<common::column_id_t>& columnIDs, const NodeGroup& localNodeGroup,
        CSRNodeGroup& csrNodeGroup, common::offset_t boundOffsetInGroup,
        const row_idx_vec_t& rowIndices, common::column_id_t skippedColumn);

    void updateRelOffsets(const LocalRelTable& localRelTable);

    static common::offset_t getCommittedOffset(common::offset_t uncommittedOffset,
        common::offset_t maxCommittedOffset);

    void detachDeleteForCSRRels(transaction::Transaction* transaction, RelTableData* tableData,
        RelTableData* reverseTableData, RelTableScanState* relDataReadState,
        RelTableDeleteState* deleteState);

    void checkRelMultiplicityConstraint(transaction::Transaction* transaction,
        const TableInsertState& state) const;

    void commitRelTableData(common::RelDataDirection direction);

private:
    common::table_id_t fromNodeTableID;
    common::table_id_t toNodeTableID;
    std::mutex relOffsetMtx;
    common::offset_t nextRelOffset;
    std::vector<std::unique_ptr<RelTableData>> directedRelData;
};

} // namespace storage
} // namespace kuzu
