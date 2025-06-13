#pragma once

#include <cstdint>

#include "common/types/types.h"
#include "storage/index/hash_index.h"
#include "storage/table/node_group_collection.h"
#include "storage/table/table.h"

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

struct KUZU_API NodeTableScanState : TableScanState {
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

// There is a vtable bug related to the Apple clang v15.0.0+. Adding the `FINAL` specifier to
// derived class causes casting failures in Apple platform.
struct KUZU_API NodeTableInsertState : public TableInsertState {
    common::ValueVector& nodeIDVector;
    const common::ValueVector& pkVector;
    std::vector<std::unique_ptr<Index::InsertState>> indexInsertStates;

    explicit NodeTableInsertState(common::ValueVector& nodeIDVector,
        const common::ValueVector& pkVector, std::vector<common::ValueVector*> propertyVectors)
        : TableInsertState{std::move(propertyVectors)}, nodeIDVector{nodeIDVector},
          pkVector{pkVector} {}

    NodeTableInsertState(const NodeTableInsertState&) = delete;
};

struct KUZU_API NodeTableUpdateState : TableUpdateState {
    common::ValueVector& nodeIDVector;
    // pkVector is nullptr if we are not updating the primary key column.
    common::ValueVector* pkVector;

    NodeTableUpdateState(common::column_id_t columnID, common::ValueVector& nodeIDVector,
        common::ValueVector& propertyVector)
        : TableUpdateState{columnID, propertyVector}, nodeIDVector{nodeIDVector},
          pkVector{nullptr} {}
};

struct KUZU_API NodeTableDeleteState : public TableDeleteState {
    common::ValueVector& nodeIDVector;
    common::ValueVector& pkVector;

    explicit NodeTableDeleteState(common::ValueVector& nodeIDVector, common::ValueVector& pkVector)
        : TableDeleteState{}, nodeIDVector{nodeIDVector}, pkVector{pkVector} {}
};

struct IndexScanHelper {
    explicit IndexScanHelper(NodeTable* table, Index* index) : table{table}, index(index) {}
    virtual ~IndexScanHelper() = default;

    virtual std::unique_ptr<NodeTableScanState> initScanState(
        const transaction::Transaction* transaction, common::DataChunk& dataChunk);
    virtual bool processScanOutput(transaction::Transaction* transaction, MemoryManager* mm,
        NodeGroupScanResult scanResult,
        const std::vector<common::ValueVector*>& scannedVectors) = 0;

    NodeTable* table;
    Index* index;
};

class NodeTableVersionRecordHandler final : public VersionRecordHandler {
public:
    explicit NodeTableVersionRecordHandler(NodeTable* table);

    void applyFuncToChunkedGroups(version_record_handler_op_t func,
        common::node_group_idx_t nodeGroupIdx, common::row_idx_t startRow,
        common::row_idx_t numRows, common::transaction_t commitTS) const override;
    void rollbackInsert(transaction::Transaction* transaction,
        common::node_group_idx_t nodeGroupIdx, common::row_idx_t startRow,
        common::row_idx_t numRows) const override;

private:
    NodeTable* table;
};

class StorageManager;

class KUZU_API NodeTable final : public Table {
public:
    NodeTable(const StorageManager* storageManager,
        const catalog::NodeTableCatalogEntry* nodeTableEntry, MemoryManager* memoryManager);

    common::row_idx_t getNumTotalRows(const transaction::Transaction* transaction) override;

    void initScanState(transaction::Transaction* transaction, TableScanState& scanState,
        bool resetCachedBoundNodeIDs = true) const override;
    void initScanState(transaction::Transaction* transaction, TableScanState& scanState,
        common::table_id_t tableID, common::offset_t startOffset) const;

    bool scanInternal(transaction::Transaction* transaction, TableScanState& scanState) override;
    bool lookup(const transaction::Transaction* transaction, const TableScanState& scanState) const;
    // TODO(Guodong): This should be merged together with `lookup`.
    void lookupMultiple(transaction::Transaction* transaction, TableScanState& scanState) const;

    // Return the max node offset during insertions.
    common::offset_t validateUniquenessConstraint(const transaction::Transaction* transaction,
        const std::vector<common::ValueVector*>& propertyVectors) const;

    void initInsertState(transaction::Transaction* transaction,
        TableInsertState& insertState) override;
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
        return getPKIndex()->appendWithIndexPos(transaction, buffer, bufferOffset, indexPos,
            [&](common::offset_t offset) { return isVisible(transaction, offset); });
    }

    void addIndex(std::unique_ptr<Index> index);
    void dropIndex(const std::string& name);

    common::column_id_t getPKColumnID() const { return pkColumnID; }
    PrimaryKeyIndex* getPKIndex() const {
        const auto index = getIndex(PrimaryKeyIndex::DEFAULT_NAME);
        KU_ASSERT(index.has_value());
        return &index.value()->cast<PrimaryKeyIndex>();
    }
    std::optional<std::reference_wrapper<IndexHolder>> getIndexHolder(const std::string& name) {
        for (auto& index : indexes) {
            if (common::StringUtils::caseInsensitiveEquals(index.getName(), name)) {
                return index;
            }
        }
        return std::nullopt;
    }
    std::optional<Index*> getIndex(const std::string& name) const {
        for (auto& index : indexes) {
            if (common::StringUtils::caseInsensitiveEquals(index.getName(), name)) {
                if (index.isLoaded()) {
                    return index.getIndex();
                }
                throw common::RuntimeException(common::stringFormat(
                    "Index {} is not loaded yet. Please load the index before accessing it.",
                    name));
            }
        }
        return std::nullopt;
    }
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
    bool checkpoint(main::ClientContext* context, catalog::TableCatalogEntry* tableEntry) override;
    void rollbackCheckpoint() override;
    void reclaimStorage(PageManager& pageManager) const override;

    void rollbackPKIndexInsert(transaction::Transaction* transaction, common::row_idx_t startRow,
        common::row_idx_t numRows_, common::node_group_idx_t nodeGroupIdx_);
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

    void serialize(common::Serializer& serializer) const override;
    void deserialize(main::ClientContext* context, StorageManager* storageManager,
        common::Deserializer& deSer) override;

private:
    void validatePkNotExists(const transaction::Transaction* transaction,
        common::ValueVector* pkVector) const;

    visible_func getVisibleFunc(const transaction::Transaction* transaction) const;
    common::DataChunk constructDataChunkForColumns(
        const std::vector<common::column_id_t>& columnIDs) const;
    void scanIndexColumns(transaction::Transaction* transaction, IndexScanHelper& scanHelper,
        const NodeGroupCollection& nodeGroups_) const;

private:
    std::vector<std::unique_ptr<Column>> columns;
    std::unique_ptr<NodeGroupCollection> nodeGroups;
    common::column_id_t pkColumnID;
    std::vector<IndexHolder> indexes;
    NodeTableVersionRecordHandler versionRecordHandler;
};

} // namespace storage
} // namespace kuzu
