#pragma once

#include "common/cast.h"
#include "common/types/types.h"
#include "storage/index/hash_index.h"
#include "storage/stats/nodes_store_statistics.h"
#include "storage/store/node_group_collection.h"
#include "storage/store/node_table_data.h"
#include "storage/store/table.h"

namespace kuzu {
namespace catalog {
class NodeTableCatalogEntry;
class Property;
} // namespace catalog

namespace transaction {
class Transaction;
} // namespace transaction

namespace storage {
class LocalNodeTable;

struct NodeTableScanState final : TableScanState {
    std::vector<ColumnPredicateSet> columnPredicateSets;
    const NodeGroup* nodeGroup = nullptr;

    explicit NodeTableScanState(const common::table_id_t tableID,
        std::vector<common::column_id_t> columnIDs)
        : TableScanState{tableID, std::move(columnIDs), std::vector<ColumnPredicateSet>{}} {}
    NodeTableScanState(common::table_id_t tableID, std::vector<common::column_id_t> columnIDs,
        std::vector<ColumnPredicateSet> columnPredicateSets)
        : TableScanState{tableID, std::move(columnIDs), std::move(columnPredicateSets)} {
        chunkStates.resize(this->columnIDs.size());
    }
};

struct NodeTableInsertState final : TableInsertState {
    common::ValueVector& nodeIDVector;
    const common::ValueVector& pkVector;

    explicit NodeTableInsertState(common::ValueVector& nodeIDVector,
        const common::ValueVector& pkVector,
        const std::vector<common::ValueVector*>& propertyVectors)
        : TableInsertState{std::move(propertyVectors)}, nodeIDVector{nodeIDVector},
          pkVector{pkVector} {}
};

struct NodeTableUpdateState final : TableUpdateState {
    common::ValueVector& nodeIDVector;
    // pkVector is nullptr if we are not updating primary key column.
    common::ValueVector* pkVector;

    NodeTableUpdateState(common::column_id_t columnID, common::ValueVector& nodeIDVector,
        common::ValueVector& propertyVector)
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
    static std::vector<common::LogicalType> getTableColumnTypes(const NodeTable& table) {
        std::vector<common::LogicalType> types;
        for (auto i = 0u; i < table.getNumColumns(); i++) {
            types.push_back(table.getColumn(i)->getDataType());
        }
        return types;
    }

    NodeTable(StorageManager* storageManager, catalog::NodeTableCatalogEntry* nodeTableEntry,
        MemoryManager* memoryManager, common::VirtualFileSystem* vfs, main::ClientContext* context,
        common::Deserializer* deSer = nullptr);

    static std::unique_ptr<NodeTable> loadTable(common::Deserializer& deSer,
        const catalog::Catalog& catalog, StorageManager* storageManager,
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
    void lookup(transaction::Transaction* transaction, TableScanState& scanState) const;

    // Return the max node offset during insertions.
    common::offset_t validateUniquenessConstraint(transaction::Transaction* transaction,
        const std::vector<common::ValueVector*>& propertyVectors) const;

    void insert(transaction::Transaction* transaction, TableInsertState& insertState) override;
    void update(transaction::Transaction* transaction, TableUpdateState& updateState) override;
    void delete_(transaction::Transaction* transaction, TableDeleteState& deleteState) override;

    void addColumn(transaction::Transaction* transaction, const catalog::Property& property,
        common::ValueVector* defaultValueVector) override;
    void dropColumn(common::column_id_t columnID) override { tableData->dropColumn(columnID); }

    common::column_id_t getPKColumnID() const { return pkColumnID; }
    PrimaryKeyIndex* getPKIndex() const { return pkIndex.get(); }
    NodesStoreStatsAndDeletedIDs* getNodeStatisticsAndDeletedIDs() const {
        return common::ku_dynamic_cast<TablesStatistics*, NodesStoreStatsAndDeletedIDs*>(
            tablesStatistics);
    }
    common::column_id_t getNumColumns() const { return tableData->getNumColumns(); }
    Column* getColumn(common::column_id_t columnID) const { return tableData->getColumn(columnID); }

    common::offset_t append(transaction::Transaction* transaction, ChunkedNodeGroup* nodeGroup,
        common::offset_t startOffsetToAppend, common::row_idx_t numRowsToAppend);

    void prepareCommit(transaction::Transaction* transaction, LocalTable* localTable) override;
    void prepareCommit() override;
    void prepareRollback(LocalTable* localTable) override;
    void checkpoint(common::Serializer& ser) override;
    void rollbackInMemory() override;

    uint64_t getEstimatedMemoryUsage() const override;

    common::node_group_idx_t getNumCommittedNodeGroups() const {
        return nodeGroups->getNumNodeGroups();
    }

    // TODO: Fix this. This is used by NodeBatchInsert.
    common::node_group_idx_t getNumNodeGroups(transaction::Transaction*) const {
        return nodeGroups->getNumNodeGroups();
    }
    // TODO: Fix this. This is used by NodeBatchInsert.
    common::offset_t getNumTuplesInNodeGroup(const transaction::Transaction*,
        common::node_group_idx_t nodeGroupIdx) const {
        return nodeGroups->getNodeGroup(nodeGroupIdx).getNumRows();
    }

private:
    void insertPK(const common::ValueVector& nodeIDVector,
        const common::ValueVector& pkVector) const;

    void checkpointInMemory() override;

    void serialize(common::Serializer& serializer) const;

private:
    std::mutex mtx;
    std::unique_ptr<NodeTableData> tableData;
    std::unique_ptr<NodeGroupCollection> nodeGroups;
    common::column_id_t pkColumnID;
    std::unique_ptr<PrimaryKeyIndex> pkIndex;
};

} // namespace storage
} // namespace kuzu
