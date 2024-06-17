#pragma once

#include "common/cast.h"
#include "common/types/types.h"
#include "processor/operator/mask.h"
#include "storage/index/hash_index.h"
#include "storage/stats/nodes_store_statistics.h"
#include "storage/store/chunked_node_group.h"
#include "storage/store/node_table_data.h"
#include "storage/store/table.h"

namespace kuzu {
namespace evaluator {
class ExpressionEvaluator;
} // namespace evaluator
namespace transaction {
class Transaction;
} // namespace transaction

namespace storage {
class LocalNodeTable;

struct NodeTableScanState final : TableScanState {
    // Local storage node group.
    LocalNodeNG* localNodeGroup = nullptr;
    processor::NodeSemiMask* semiMask;

    explicit NodeTableScanState(std::vector<common::column_id_t> columnIDs)
        : TableScanState{std::move(columnIDs), std::vector<ColumnPredicateSet>{}} {
        dataScanState = std::make_unique<NodeDataScanState>(this->columnIDs);
    }
    NodeTableScanState(std::vector<common::column_id_t> columnIDs,
        std::vector<ColumnPredicateSet> columnPredicateSets, processor::NodeSemiMask* semiMask)
        : TableScanState{std::move(columnIDs), std::move(columnPredicateSets)}, semiMask{semiMask} {
        dataScanState = std::make_unique<NodeDataScanState>(this->columnIDs);
    }
};

struct NodeTableInsertState final : TableInsertState {
    common::ValueVector& nodeIDVector;
    const common::ValueVector& pkVector;

    NodeTableInsertState(common::ValueVector& nodeIDVector, const common::ValueVector& pkVector,
        const std::vector<common::ValueVector*>& propertyVectors)
        : TableInsertState{std::move(propertyVectors)}, nodeIDVector{nodeIDVector},
          pkVector{pkVector} {}
};

struct NodeTableUpdateState final : TableUpdateState {
    common::ValueVector& nodeIDVector;
    // pkVector is nullptr if we are not updating primary key column.
    common::ValueVector* pkVector;

    NodeTableUpdateState(common::column_id_t columnID, common::ValueVector& nodeIDVector,
        const common::ValueVector& propertyVector)
        : TableUpdateState{columnID, propertyVector}, nodeIDVector{nodeIDVector},
          pkVector{nullptr} {}
};

struct NodeTableDeleteState final : TableDeleteState {
    common::ValueVector& nodeIDVector;
    common::ValueVector& pkVector;

    explicit NodeTableDeleteState(common::ValueVector& nodeIDVector, common::ValueVector& pkVector)
        : nodeIDVector{nodeIDVector}, pkVector{pkVector} {}
};

class StorageManager;
class NodeTable final : public Table {
public:
    NodeTable(StorageManager* storageManager, catalog::NodeTableCatalogEntry* nodeTableEntry,
        MemoryManager* memoryManager, common::VirtualFileSystem* vfs, main::ClientContext* context);

    void initializePKIndex(const std::string& databasePath,
        const catalog::NodeTableCatalogEntry* nodeTableEntry, bool readOnly,
        common::VirtualFileSystem* vfs, main::ClientContext* context);

    common::offset_t getMaxNodeOffset(transaction::Transaction* transaction) const {
        const auto nodesStats =
            common::ku_dynamic_cast<TablesStatistics*, NodesStoreStatsAndDeletedIDs*>(
                tablesStatistics);
        return nodesStats->getMaxNodeOffset(transaction, tableID);
    }

    void initializeScanState(transaction::Transaction* transaction,
        TableScanState& scanState) const override;

    bool scanInternal(transaction::Transaction* transaction, TableScanState& scanState) override;
    void lookup(transaction::Transaction* transaction, TableScanState& scanState);

    // Return the max node offset during insertions.
    common::offset_t validateUniquenessConstraint(transaction::Transaction* transaction,
        const std::vector<common::ValueVector*>& propertyVectors) const;

    void insert(transaction::Transaction* transaction, TableInsertState& insertState) override;
    void update(transaction::Transaction* transaction, TableUpdateState& updateState) override;
    bool delete_(transaction::Transaction* transaction, TableDeleteState& deleteState) override;

    void addColumn(transaction::Transaction* transaction, const catalog::Property& property,
        evaluator::ExpressionEvaluator& defaultEvaluator) override;
    void dropColumn(common::column_id_t columnID) override { tableData->dropColumn(columnID); }

    common::column_id_t getPKColumnID() const { return pkColumnID; }
    PrimaryKeyIndex* getPKIndex() const { return pkIndex.get(); }
    NodesStoreStatsAndDeletedIDs* getNodeStatisticsAndDeletedIDs() const {
        return common::ku_dynamic_cast<TablesStatistics*, NodesStoreStatsAndDeletedIDs*>(
            tablesStatistics);
    }
    common::column_id_t getNumColumns() const { return tableData->getNumColumns(); }
    Column* getColumn(common::column_id_t columnID) const { return tableData->getColumn(columnID); }

    void append(transaction::Transaction* transaction, ChunkedNodeGroup* nodeGroup) {
        tableData->append(transaction, nodeGroup);
    }

    void prepareCommitNodeGroup(common::node_group_idx_t nodeGroupIdx,
        transaction::Transaction* transaction, LocalNodeNG* localNodeGroup) const;
    void prepareCommit(transaction::Transaction* transaction, LocalTable* localTable) override;
    void prepareCommit() override;
    void prepareRollback(LocalTable* localTable) override;
    void checkpointInMemory() override;
    void rollbackInMemory() override;

    common::node_group_idx_t getNumCommittedNodeGroups() const {
        return tableData->getNumCommittedNodeGroups();
    }

    common::node_group_idx_t getNumNodeGroups(transaction::Transaction* transaction) const {
        return tableData->getNumNodeGroups(transaction);
    }
    common::offset_t getNumTuplesInNodeGroup(const transaction::Transaction* transaction,
        common::node_group_idx_t nodeGroupIdx) const {
        return tableData->getNumTuplesInNodeGroup(transaction, nodeGroupIdx);
    }

private:
    void insertPK(const common::ValueVector& nodeIDVector,
        const common::ValueVector& pkVector) const;
    bool scanCommitted(transaction::Transaction* transaction, NodeTableScanState& scanState);
    bool scanUnCommitted(NodeTableScanState& scanState);

private:
    std::unique_ptr<NodeTableData> tableData;
    common::column_id_t pkColumnID;
    std::unique_ptr<PrimaryKeyIndex> pkIndex;
};

} // namespace storage
} // namespace kuzu
