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

struct KUZU_API NodeTableScanState final : TableScanState {
    NodeTableScanState(common::ValueVector* nodeIDVector,
        std::vector<common::ValueVector*> outputVectors,
        std::shared_ptr<common::DataChunkState> outChunkState)
        : TableScanState{nodeIDVector, std::move(outputVectors), std::move(outChunkState)} {
        nodeGroupScanState = std::make_unique<NodeGroupScanState>(this->columnIDs.size());
    }

    void setToTable(const transaction::Transaction* transaction, Table* table_,
        std::vector<common::column_id_t> columnIDs_,
        std::vector<ColumnPredicateSet> columnPredicateSets_ = {},
        common::RelDataDirection direction = common::RelDataDirection::INVALID) override;

    bool scanNext(transaction::Transaction* transaction) override;

    bool scanNext(transaction::Transaction* transaction, common::offset_t startOffset,
        common::offset_t numNodes);
};

struct NodeTableInsertState final : TableInsertState {
    common::ValueVector& nodeIDVector;
    const common::ValueVector& pkVector;

    explicit NodeTableInsertState(common::ValueVector& nodeIDVector,
        const common::ValueVector& pkVector, std::vector<common::ValueVector*> propertyVectors)
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
    explicit PKColumnScanHelper(NodeTable* table, PrimaryKeyIndex* pkIndex)
        : table{table}, pkIndex(pkIndex) {}
    virtual ~PKColumnScanHelper() = default;

    virtual std::unique_ptr<NodeTableScanState> initPKScanState(
        const transaction::Transaction* transaction, common::DataChunk& dataChunk,
        common::column_id_t pkColumnID);
    virtual bool processScanOutput(const transaction::Transaction* transaction,
        NodeGroupScanResult scanResult, const common::ValueVector& scannedVector) = 0;

    NodeTable* table;
    PrimaryKeyIndex* pkIndex;
};

class NodeTableVersionRecordHandler final : public VersionRecordHandler {
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

class KUZU_API NodeTable final : public Table {
public:
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

    void initScanState(transaction::Transaction* transaction, TableScanState& scanState,
        bool resetCachedBoundNodeIDs = true) const override;
    void initScanState(transaction::Transaction* transaction, TableScanState& scanState,
        common::table_id_t tableID, common::offset_t startOffset) const;

    bool scanInternal(transaction::Transaction* transaction, TableScanState& scanState) override;
    bool lookup(const transaction::Transaction* transaction, const TableScanState& scanState) const;

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

    std::pair<common::offset_t, common::offset_t> appendToLastNodeGroup(MemoryManager& mm,
        transaction::Transaction* transaction, const std::vector<common::column_id_t>& columnIDs,
        ChunkedNodeGroup& chunkedGroup);

    void commit(transaction::Transaction* transaction, catalog::TableCatalogEntry* tableEntry,
        LocalTable* localTable) override;
    void checkpoint(common::Serializer& ser, catalog::TableCatalogEntry* tableEntry) override;
    void rollbackCheckpoint() override;

    void rollbackPKIndexInsert(const transaction::Transaction* transaction,
        common::row_idx_t startRow, common::row_idx_t numRows_,
        common::node_group_idx_t nodeGroupIdx_);
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
    // NOLINTNEXTLINE(readability-make-member-function-const): Semantically non-const.
    void mergeStats(const std::vector<common::column_id_t>& columnIDs, const TableStats& stats) {
        nodeGroups->mergeStats(columnIDs, stats);
    }

private:
    void validatePkNotExists(const transaction::Transaction* transaction,
        common::ValueVector* pkVector) const;

    void serialize(common::Serializer& serializer) const override;

    visible_func getVisibleFunc(const transaction::Transaction* transaction) const;
    common::DataChunk constructDataChunkForPKColumn() const;
    void scanPKColumn(const transaction::Transaction* transaction, PKColumnScanHelper& scanHelper,
        const NodeGroupCollection& nodeGroups_) const;

private:
    std::vector<std::unique_ptr<Column>> columns;
    std::unique_ptr<NodeGroupCollection> nodeGroups;
    common::column_id_t pkColumnID;
    std::unique_ptr<PrimaryKeyIndex> pkIndex;
    NodeTableVersionRecordHandler versionRecordHandler;
};

} // namespace storage
} // namespace kuzu
