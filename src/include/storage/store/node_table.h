#pragma once

#include <cstdint>

#include "common/types/types.h"
#include "storage/index/hash_index.h"
#include "storage/store/node_group_collection.h"
#include "storage/store/table.h"

namespace kuzu {
namespace evaluator {
class ExpressionEvaluator;
} // namespace evaluator

namespace catalog {
class NodeTableCatalogEntry;
class Property;
} // namespace catalog

namespace transaction {
class Transaction;
} // namespace transaction

namespace storage {

struct NodeTableScanState final : TableScanState {
    // Scan state for un-committed data.
    // Ideally we shouldn't need columns to scan un-checkpointed but committed data.
    NodeTableScanState(common::table_id_t tableID, std::vector<common::column_id_t> columnIDs)
        : NodeTableScanState{tableID, std::move(columnIDs), {}} {}
    NodeTableScanState(common::table_id_t tableID, std::vector<common::column_id_t> columnIDs,
        std::vector<const Column*> columns)
        : NodeTableScanState{tableID, std::move(columnIDs), std::move(columns),
              std::vector<ColumnPredicateSet>{}} {}
    NodeTableScanState(common::table_id_t tableID, std::vector<common::column_id_t> columnIDs,
        std::vector<const Column*> columns, std::vector<ColumnPredicateSet> columnPredicateSets)
        : TableScanState{tableID, std::move(columnIDs), std::move(columns),
              std::move(columnPredicateSets)} {
        nodeGroupScanState = std::make_unique<NodeGroupScanState>(this->columnIDs.size());
    }

    bool scanNext(transaction::Transaction* transaction) override;
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
    static std::vector<common::LogicalType> getNodeTableColumnTypes(const NodeTable& table) {
        std::vector<common::LogicalType> types;
        for (auto i = 0u; i < table.getNumColumns(); i++) {
            types.push_back(table.getColumn(i).getDataType().copy());
        }
        return types;
    }

    NodeTable(const StorageManager* storageManager,
        const catalog::NodeTableCatalogEntry* nodeTableEntry, MemoryManager* memoryManager,
        common::VirtualFileSystem* vfs, main::ClientContext* context,
        common::Deserializer* deSer = nullptr);

    static std::unique_ptr<NodeTable> loadTable(common::Deserializer& deSer,
        const catalog::Catalog& catalog, StorageManager* storageManager,
        MemoryManager* memoryManager, common::VirtualFileSystem* vfs, main::ClientContext* context);

    void initializePKIndex(const std::string& databasePath,
        const catalog::NodeTableCatalogEntry* nodeTableEntry, bool readOnly,
        common::VirtualFileSystem* vfs, main::ClientContext* context);

    common::row_idx_t getNumTotalRows(const transaction::Transaction* transaction) override;

    void initScanState(transaction::Transaction* transaction,
        TableScanState& scanState) const override;

    bool scanInternal(transaction::Transaction* transaction, TableScanState& scanState) override;
    bool lookup(transaction::Transaction* transaction, const TableScanState& scanState) const;

    // Return the max node offset during insertions.
    common::offset_t validateUniquenessConstraint(const transaction::Transaction* transaction,
        const std::vector<common::ValueVector*>& propertyVectors) const;

    void insert(transaction::Transaction* transaction, TableInsertState& insertState) override;
    void update(transaction::Transaction* transaction, TableUpdateState& updateState) override;
    bool delete_(transaction::Transaction* transaction, TableDeleteState& deleteState) override;

    void addColumn(transaction::Transaction* transaction,
        TableAddColumnState& addColumnState) override;
    bool isVisible(const transaction::Transaction* transaction, common::offset_t offset) const;
    bool isVisibleNoLock(const transaction::Transaction* transaction,
        common::offset_t offset) const;

    bool lookupPK(const transaction::Transaction* transaction, common::ValueVector* keyVector,
        uint64_t vectorPos, common::offset_t& result) const;
    template<common::IndexHashable T>
    size_t appendPKWithIndexPos(const transaction::Transaction* transaction,
        const IndexBuffer<T>& buffer, uint64_t bufferOffset, uint64_t indexPos) {
        return pkIndex->appendWithIndexPos(transaction, buffer, bufferOffset, indexPos,
            [&](common::offset_t offset) { return isVisible(transaction, offset); });
    }

    common::column_id_t getPKColumnID() const { return pkColumnID; }
    PrimaryKeyIndex* getPKIndex() const { return pkIndex.get(); }
    common::column_id_t getNumColumns() const { return columns.size(); }
    Column& getColumn(common::column_id_t columnID) {
        KU_ASSERT(columnID < columns.size());
        return *columns[columnID];
    }
    const Column& getColumn(common::column_id_t columnID) const {
        KU_ASSERT(columnID < columns.size());
        return *columns[columnID];
    }

    std::pair<common::offset_t, common::offset_t> appendToLastNodeGroup(
        transaction::Transaction* transaction, ChunkedNodeGroup& chunkedGroup);

    void commit(transaction::Transaction* transaction, LocalTable* localTable) override;
    void checkpoint(common::Serializer& ser, catalog::TableCatalogEntry* tableEntry) override;

    common::node_group_idx_t getNumCommittedNodeGroups() const {
        return nodeGroups->getNumNodeGroups();
    }

    common::node_group_idx_t getNumNodeGroups() const { return nodeGroups->getNumNodeGroups(); }
    common::offset_t getNumTuplesInNodeGroup(common::node_group_idx_t nodeGroupIdx) const {
        return nodeGroups->getNodeGroup(nodeGroupIdx)->getNumRows();
    }
    NodeGroup* getNodeGroup(common::node_group_idx_t nodeGroupIdx) const {
        return nodeGroups->getNodeGroup(nodeGroupIdx);
    }
    NodeGroup* getNodeGroupNoLock(common::node_group_idx_t nodeGroupIdx) const {
        return nodeGroups->getNodeGroupNoLock(nodeGroupIdx);
    }

    TableStats getStats(const transaction::Transaction* transaction) const;

private:
    void insertPK(const transaction::Transaction* transaction,
        const common::ValueVector& nodeIDVector, const common::ValueVector& pkVector) const;
    void validatePkNotExists(const transaction::Transaction* transaction,
        common::ValueVector* pkVector);

    void serialize(common::Serializer& serializer) const override;

private:
    std::vector<std::unique_ptr<Column>> columns;
    std::unique_ptr<NodeGroupCollection> nodeGroups;
    common::column_id_t pkColumnID;
    std::unique_ptr<PrimaryKeyIndex> pkIndex;
};

} // namespace storage
} // namespace kuzu
