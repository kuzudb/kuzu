#pragma once

#include "catalog/catalog_entry/rel_table_catalog_entry.h"
#include "storage/store/csr_node_group_collection.h"
#include "storage/store/rel_table_data.h"
#include "storage/store/table.h"

namespace kuzu {
namespace storage {

struct RelTableScanState final : TableScanState {
    common::RelDataDirection direction;
    common::ValueVector* relIDVector;
    common::offset_t boundNodeOffset;
    Column* csrOffsetColumn;
    Column* csrLengthColumn;

    // When scanning from local storage, nodeGroup is a normal node group.
    // When scanning from committed data, nodeGroup is a CSRNodeGroup.

    // Scan state for un-committed data.
    // Ideally we shouldn't need columns to scan un-checkpointed but committed data.
    RelTableScanState(common::table_id_t tableID, const std::vector<common::column_id_t>& columnIDs)
        : RelTableScanState(tableID, columnIDs, {}, nullptr, nullptr,
              common::RelDataDirection::FWD /* This is a dummy direction */,
              std::vector<ColumnPredicateSet>{}) {
        nodeGroupScanState = std::make_unique<CSRNodeGroupScanState>(this->columnIDs.size());
    }

    RelTableScanState(common::table_id_t tableID, const std::vector<common::column_id_t>& columnIDs,
        const std::vector<Column*>& columns, Column* csrOffsetCol, Column* csrLengthCol,
        common::RelDataDirection direction)
        : RelTableScanState(tableID, columnIDs, columns, csrOffsetCol, csrLengthCol, direction,
              std::vector<ColumnPredicateSet>{}) {
        nodeGroupScanState = std::make_unique<CSRNodeGroupScanState>(this->columnIDs.size());
    }
    RelTableScanState(common::table_id_t tableID, const std::vector<common::column_id_t>& columnIDs,
        const std::vector<Column*>& columns, Column* csrOffsetCol, Column* csrLengthCol,
        common::RelDataDirection direction, std::vector<ColumnPredicateSet> columnPredicateSets)
        : TableScanState{tableID, columnIDs, columns, std::move(columnPredicateSets)},
          direction{direction}, relIDVector{nullptr}, boundNodeOffset{common::INVALID_OFFSET},
          csrOffsetColumn{csrOffsetCol}, csrLengthColumn{csrLengthCol} {
        nodeGroupScanState = std::make_unique<CSRNodeGroupScanState>(this->columnIDs.size());
        // TODO(Guodong): Move the NBR_ID_COLUMN_ID to binder phase.
        // std::vector<common::column_id_t> dataScanColumnIDs{NBR_ID_COLUMN_ID};
        // dataScanColumnIDs.insert(dataScanColumnIDs.end(), columnIDs.begin(), columnIDs.end());
        if (!this->columnPredicateSets.empty()) {
            // Since we insert a nbr column. We need to pad an empty nbr column predicate set.
            this->columnPredicateSets.insert(this->columnPredicateSets.begin(),
                ColumnPredicateSet());
        }
    }

    void resetState() override {
        boundNodeOffset = common::INVALID_OFFSET;
        nodeGroupScanState->resetState();
    }

    // bool hasMoreToRead(const transaction::Transaction* transaction) const {
    // First, check if we have more to read in the committed node group. including checkpointed
    // and non-checkpointed data.
    // Then, check if we have more to read in the uncommitted node groups.
    // }
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
    const common::ValueVector& srcNodeIDVector;
    const common::ValueVector& dstNodeIDVector;
    const common::ValueVector& relIDVector;

    RelTableUpdateState(common::column_id_t columnID, const common::ValueVector& srcNodeIDVector,
        const common::ValueVector& dstNodeIDVector, const common::ValueVector& relIDVector,
        common::ValueVector& propertyVector)
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
    RelTable(catalog::RelTableCatalogEntry* relTableEntry, StorageManager* storageManager,
        MemoryManager* memoryManager, common::Deserializer* deSer = nullptr);

    static std::unique_ptr<RelTable> loadTable(common::Deserializer& deSer,
        const catalog::Catalog& catalog, StorageManager* storageManager,
        MemoryManager* memoryManager, common::VirtualFileSystem* vfs, main::ClientContext* context);

    void initializeScanState(transaction::Transaction* transaction,
        TableScanState& scanState) override;

    bool scanInternal(transaction::Transaction* transaction, TableScanState& scanState) override;

    void insert(transaction::Transaction* transaction, TableInsertState& insertState) override;
    void update(transaction::Transaction* transaction, TableUpdateState& updateState) override;
    void delete_(transaction::Transaction* transaction, TableDeleteState& deleteState) override;

    void detachDelete(transaction::Transaction* transaction, common::RelDataDirection direction,
        common::ValueVector* srcNodeIDVector, RelDetachDeleteState* deleteState);
    void checkIfNodeHasRels(transaction::Transaction* transaction,
        common::RelDataDirection direction, common::ValueVector* srcNodeIDVector) const;

    void addColumn(transaction::Transaction* transaction, const catalog::Property& property,
        common::ValueVector* defaultValueVector) override;
    void dropColumn(common::column_id_t) override {
        // fwdRelTableData->dropColumn(columnID);
        // bwdRelTableData->dropColumn(columnID);
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
    // const std::vector<std::unique_ptr<Column>>& getColumns(
    // common::RelDataDirection direction) const {
    // return direction == common::RelDataDirection::FWD ? fwdRelTableData->getColumns() :
    // bwdRelTableData->getColumns();
    // }

    void append(transaction::Transaction* transaction, common::node_group_idx_t nodeGroupIdx,
        ChunkedNodeGroup* nodeGroup, common::RelDataDirection direction) {
        direction == common::RelDataDirection::FWD ?
            fwdRelTableData->append(transaction, nodeGroupIdx, nodeGroup) :
            bwdRelTableData->append(transaction, nodeGroupIdx, nodeGroup);
    }
    NodeGroup* getOrCreateNodeGroup(common::node_group_idx_t nodeGroupIdx,
        common::RelDataDirection direction) const;

    bool isNewNodeGroup(transaction::Transaction* transaction,
        common::node_group_idx_t nodeGroupIdx, common::RelDataDirection direction) const {
        return direction == common::RelDataDirection::FWD ?
                   fwdRelTableData->isNewNodeGroup(transaction, nodeGroupIdx) :
                   bwdRelTableData->isNewNodeGroup(transaction, nodeGroupIdx);
    }

    void prepareCommit(transaction::Transaction* transaction, LocalTable* localTable) override;
    void prepareRollback(LocalTable* localTable) override;
    void checkpoint(common::Serializer& ser) override;

    uint64_t getEstimatedMemoryUsage() const override { return 0; }

    common::row_idx_t getNumRows(transaction::Transaction* transaction) override {
        const auto numRows = fwdRelTableData->getNumRows(transaction);
        KU_ASSERT(numRows == bwdRelTableData->getNumRows(transaction));
        return numRows;
    }

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
    common::row_idx_t detachDeleteForCSRRels(transaction::Transaction* transaction,
        RelTableData* tableData, RelTableData* reverseTableData,
        common::ValueVector* srcNodeIDVector, RelTableScanState* relDataReadState,
        RelDetachDeleteState* deleteState);

    void serialize(common::Serializer& serializer) const override;

private:
    std::mutex relOffsetMtx;
    common::offset_t nextRelOffset;
    std::unique_ptr<RelTableData> fwdRelTableData;
    std::unique_ptr<RelTableData> bwdRelTableData;
};

} // namespace storage
} // namespace kuzu
