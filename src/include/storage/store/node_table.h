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
} // namespace catalog

namespace transaction {
class Transaction;
} // namespace transaction

namespace storage {
class NodeTable;

struct NodeTableScanState final : TableScanState {
    // Scan state for un-committed data.
    // Ideally we shouldn't need columns to scan un-checkpointed but committed data.
    NodeTableScanState(common::table_id_t tableID, std::vector<common::column_id_t> columnIDs,
        std::vector<const Column*> columns = {},
        std::vector<ColumnPredicateSet> columnPredicateSets = {})
        : TableScanState{tableID, std::move(columnIDs), std::move(columns),
              std::move(columnPredicateSets)} {
        nodeGroupScanState = std::make_unique<NodeGroupScanState>(this->columnIDs.size());
    }

    NodeTableScanState(common::table_id_t tableID, std::vector<common::column_id_t> columnIDs,
        std::vector<const Column*> columns, const common::DataChunk& dataChunk,
        common::ValueVector* nodeIDVector)
        : NodeTableScanState{tableID, std::move(columnIDs), std::move(columns)} {
        for (auto& vector : dataChunk.valueVectors) {
            outputVectors.push_back(vector.get());
        }
        outState = dataChunk.state.get();
        this->nodeIDVector = nodeIDVector;
        rowIdxVector->state = this->nodeIDVector->state;
    }

    bool scanNext(transaction::Transaction* transaction) override;

    bool scanNext(transaction::Transaction* transaction, common::offset_t startOffset,
        common::offset_t numNodes);
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

struct PKColumnScanHelper {
    explicit PKColumnScanHelper(PrimaryKeyIndex* pkIndex, common::table_id_t tableID)
        : tableID(tableID), pkIndex(pkIndex) {}
    virtual ~PKColumnScanHelper() = default;

    virtual std::unique_ptr<NodeTableScanState> initPKScanState(common::DataChunk& dataChunk,
        common::column_id_t pkColumnID, const std::vector<std::unique_ptr<Column>>& columns);
    virtual bool processScanOutput(const transaction::Transaction* transaction,
        NodeGroupScanResult scanResult, const common::ValueVector& scannedVector) = 0;

    common::table_id_t tableID;
    PrimaryKeyIndex* pkIndex;
};

class NodeTableVersionRecordHandler : public VersionRecordHandler {
public:
    explicit NodeTableVersionRecordHandler(NodeTable* table);

    void applyFuncToChunkedGroups(version_record_handler_op_t func,
        common::node_group_idx_t nodeGroupIdx, common::row_idx_t startRow,
        common::row_idx_t numRows, common::transaction_t commitTS) const override;
    void rollbackInsert(const transaction::Transaction* transaction,
        common::node_group_idx_t nodeGroupIdx, common::row_idx_t startRow,
        common::row_idx_t numRows) const override;

private:
    NodeTable* table;
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
    void initScanState(transaction::Transaction* transaction, TableScanState& scanState,
        common::table_id_t tableID, common::offset_t startOffset) const;

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
    void rollbackCheckpoint() override;

    void rollbackPKIndexInsert(const transaction::Transaction* transaction,
        common::row_idx_t startRow, common::row_idx_t numRows_,
        common::node_group_idx_t nodeGroupIdx);
    void rollbackGroupCollectionInsert(common::row_idx_t numRows_);

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
    void mergeStats(const TableStats& stats) { nodeGroups->mergeStats(stats); }

private:
    void validatePkNotExists(const transaction::Transaction* transaction,
        common::ValueVector* pkVector);

    void serialize(common::Serializer& serializer) const override;

    visible_func getVisibleFunc(const transaction::Transaction* transaction) const;
    common::DataChunk constructDataChunkForPKColumn() const;
    void scanPKColumn(const transaction::Transaction* transaction, PKColumnScanHelper& scanHelper,
        NodeGroupCollection& nodeGroups_);

private:
    std::vector<std::unique_ptr<Column>> columns;
    std::unique_ptr<NodeGroupCollection> nodeGroups;
    common::column_id_t pkColumnID;
    std::unique_ptr<PrimaryKeyIndex> pkIndex;
    NodeTableVersionRecordHandler versionRecordHandler;
};

} // namespace storage
} // namespace kuzu
