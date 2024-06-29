#pragma once

#include "catalog/catalog_entry/rel_table_catalog_entry.h"
#include "storage/store/rel_table_data.h"
#include "storage/store/table.h"

namespace kuzu {
namespace evaluator {
class ExpressionEvaluator;
} // namespace evaluator
namespace storage {

struct RelTableScanState final : TableScanState {
    common::RelDataDirection direction;

    RelTableScanState(const std::vector<common::column_id_t>& columnIDs,
        common::RelDataDirection direction)
        : RelTableScanState(columnIDs, direction, std::vector<ColumnPredicateSet>{}) {}
    RelTableScanState(const std::vector<common::column_id_t>& columnIDs,
        common::RelDataDirection direction, std::vector<ColumnPredicateSet> columnPredicateSets)
        : TableScanState{columnIDs, std::move(columnPredicateSets)}, direction{direction} {
        // TODO(Guodong): Move the NBR_ID_COLUMN_ID to binder phase.
        std::vector<common::column_id_t> dataScanColumnIDs{NBR_ID_COLUMN_ID};
        dataScanColumnIDs.insert(dataScanColumnIDs.end(), columnIDs.begin(), columnIDs.end());
        if (!this->columnPredicateSets.empty()) {
            // Since we insert a nbr column. We need to pad an empty nbr column predicate set.
            this->columnPredicateSets.insert(this->columnPredicateSets.begin(),
                ColumnPredicateSet());
        }
        dataScanState = std::make_unique<RelDataReadState>(dataScanColumnIDs);
    }

    bool hasMoreToRead(const transaction::Transaction* transaction) const {
        return common::ku_dynamic_cast<TableDataScanState*, RelDataReadState*>(dataScanState.get())
            ->hasMoreToRead(transaction);
    }
};

struct RelTableInsertState final : TableInsertState {
    const common::ValueVector& srcNodeIDVector;
    const common::ValueVector& dstNodeIDVector;

    RelTableInsertState(const common::ValueVector& srcNodeIDVector,
        const common::ValueVector& dstNodeIDVector,
        const std::vector<common::ValueVector*>& propertyVectors)
        : TableInsertState{std::move(propertyVectors)}, srcNodeIDVector{srcNodeIDVector},
          dstNodeIDVector{dstNodeIDVector} {}
};

struct RelTableUpdateState final : TableUpdateState {
    const common::ValueVector& srcNodeIDVector;
    const common::ValueVector& dstNodeIDVector;
    const common::ValueVector& relIDVector;

    RelTableUpdateState(common::column_id_t columnID, const common::ValueVector& srcNodeIDVector,
        const common::ValueVector& dstNodeIDVector, const common::ValueVector& relIDVector,
        const common::ValueVector& propertyVector)
        : TableUpdateState{columnID, propertyVector}, srcNodeIDVector{srcNodeIDVector},
          dstNodeIDVector{dstNodeIDVector}, relIDVector{relIDVector} {}
};

struct RelTableDeleteState final : TableDeleteState {
    const common::ValueVector& srcNodeIDVector;
    const common::ValueVector& dstNodeIDVector;
    const common::ValueVector& relIDVector;

    RelTableDeleteState(const common::ValueVector& srcNodeIDVector,
        const common::ValueVector& dstNodeIDVector, const common::ValueVector& relIDVector)
        : srcNodeIDVector{srcNodeIDVector}, dstNodeIDVector{dstNodeIDVector},
          relIDVector{relIDVector} {}
};

// TODO(Guodong): Should move inside RelTableDeleteState.
struct RelDetachDeleteState {
    std::unique_ptr<common::ValueVector> dstNodeIDVector;
    std::unique_ptr<common::ValueVector> relIDVector;

    explicit RelDetachDeleteState();
};

class RelsStoreStats;
class RelTable final : public Table {
public:
    RelTable(BMFileHandle* dataFH, DiskArrayCollection* metadataDAC, RelsStoreStats* relsStoreStats,
        MemoryManager* memoryManager, catalog::RelTableCatalogEntry* relTableEntry, WAL* wal,
        bool enableCompression);

    common::table_id_t getToNodeTableID() const { return toNodeTableID; }

    void initializeScanState(transaction::Transaction* transaction,
        TableScanState& scanState) const override;

    bool scanInternal(transaction::Transaction* transaction, TableScanState& scanState) override;

    void insert(transaction::Transaction* transaction, TableInsertState& insertState) override;
    void update(transaction::Transaction* transaction, TableUpdateState& updateState) override;
    bool delete_(transaction::Transaction* transaction, TableDeleteState& deleteState) override;

    void detachDelete(transaction::Transaction* transaction, common::RelDataDirection direction,
        common::ValueVector* srcNodeIDVector, RelDetachDeleteState* deleteState);
    void checkIfNodeHasRels(transaction::Transaction* transaction,
        common::RelDataDirection direction, common::ValueVector* srcNodeIDVector);

    void addColumn(transaction::Transaction* transaction, const catalog::Property& property,
        evaluator::ExpressionEvaluator& defaultEvaluator) override;
    void dropColumn(common::column_id_t columnID) override {
        fwdRelTableData->dropColumn(columnID);
        bwdRelTableData->dropColumn(columnID);
    }
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
    const std::vector<std::unique_ptr<Column>>& getColumns(
        common::RelDataDirection direction) const {
        return direction == common::RelDataDirection::FWD ? fwdRelTableData->getColumns() :
                                                            bwdRelTableData->getColumns();
    }

    void append(transaction::Transaction* transaction, ChunkedNodeGroup* nodeGroup,
        common::RelDataDirection direction) {
        direction == common::RelDataDirection::FWD ?
            fwdRelTableData->append(transaction, nodeGroup) :
            bwdRelTableData->append(transaction, nodeGroup);
    }

    bool isNewNodeGroup(transaction::Transaction* transaction,
        common::node_group_idx_t nodeGroupIdx, common::RelDataDirection direction) const {
        return direction == common::RelDataDirection::FWD ?
                   fwdRelTableData->isNewNodeGroup(transaction, nodeGroupIdx) :
                   bwdRelTableData->isNewNodeGroup(transaction, nodeGroupIdx);
    }

    void prepareCommit(transaction::Transaction* transaction, LocalTable* localTable) override;
    void prepareCommit() override;
    void prepareRollback(LocalTable* localTable) override;
    void checkpointInMemory() override;
    void rollbackInMemory() override;

    RelTableData* getDirectedTableData(common::RelDataDirection direction) const {
        return direction == common::RelDataDirection::FWD ? fwdRelTableData.get() :
                                                            bwdRelTableData.get();
    }

private:
    common::row_idx_t detachDeleteForCSRRels(transaction::Transaction* transaction,
        RelTableData* tableData, RelTableData* reverseTableData,
        common::ValueVector* srcNodeIDVector, RelTableScanState* relDataReadState,
        RelDetachDeleteState* deleteState);

private:
    // Note: Only toNodeTableID is needed for now. Expose fromNodeTableID if needed.
    common::table_id_t toNodeTableID;
    std::unique_ptr<RelTableData> fwdRelTableData;
    std::unique_ptr<RelTableData> bwdRelTableData;
};

} // namespace storage
} // namespace kuzu
