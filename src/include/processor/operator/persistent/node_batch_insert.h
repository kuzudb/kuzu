#pragma once

#include "common/cast.h"
#include "common/types/internal_id_t.h"
#include "common/types/types.h"
#include "processor/operator/aggregate/hash_aggregate.h"
#include "processor/operator/call/in_query_call.h"
#include "processor/operator/persistent/batch_insert.h"
#include "processor/operator/persistent/index_builder.h"
#include "storage/store/chunked_node_group.h"
#include "storage/store/node_table.h"

namespace kuzu {
namespace transaction {
class Transaction;
};
namespace processor {
struct ExecutionContext;

struct NodeBatchInsertInfo final : public BatchInsertInfo {
    std::vector<DataPos> columnPositions;
    bool containSerial = false;
    std::vector<common::LogicalType> columnTypes;

    NodeBatchInsertInfo(catalog::TableCatalogEntry* tableEntry, bool compressionEnabled,
        std::vector<DataPos> columnPositions, bool containSerial,
        std::vector<common::LogicalType> columnTypes)
        : BatchInsertInfo{tableEntry, compressionEnabled}, columnPositions{columnPositions},
          containSerial{containSerial}, columnTypes{std::move(columnTypes)} {}

    NodeBatchInsertInfo(const NodeBatchInsertInfo& other)
        : BatchInsertInfo{other.tableEntry, other.compressionEnabled},
          columnPositions{other.columnPositions}, containSerial{other.containSerial},
          columnTypes{common::LogicalType::copy(other.columnTypes)} {}

    inline std::unique_ptr<BatchInsertInfo> copy() const override {
        return std::make_unique<NodeBatchInsertInfo>(*this);
    }
};

struct NodeBatchInsertSharedState final : public BatchInsertSharedState {
    // Primary key info
    storage::PrimaryKeyIndex* pkIndex;
    common::vector_idx_t pkColumnIdx;
    common::LogicalType pkType;
    std::optional<IndexBuilder> globalIndexBuilder = std::nullopt;

    InQueryCallSharedState* readerSharedState;
    HashAggregateSharedState* distinctSharedState;

    uint64_t currentNodeGroupIdx;
    // The sharedNodeGroup is to accumulate left data within local node groups in NodeBatchInsert
    // ops.
    std::unique_ptr<storage::ChunkedNodeGroup> sharedNodeGroup;

    NodeBatchInsertSharedState(storage::Table* table, std::shared_ptr<FactorizedTable> fTable,
        storage::WAL* wal)
        : BatchInsertSharedState{table, fTable, wal}, readerSharedState{nullptr},
          distinctSharedState{nullptr}, currentNodeGroupIdx{0}, sharedNodeGroup{nullptr} {
        pkIndex =
            common::ku_dynamic_cast<storage::Table*, storage::NodeTable*>(table)->getPKIndex();
    }

    void initPKIndex(ExecutionContext* context);

    inline common::offset_t getNextNodeGroupIdx() {
        std::unique_lock lck{mtx};
        return getNextNodeGroupIdxWithoutLock();
    }

    inline uint64_t getCurNodeGroupIdx() const { return currentNodeGroupIdx; }

    inline common::offset_t getNextNodeGroupIdxWithoutLock() { return currentNodeGroupIdx++; }

    void calculateNumTuples();
};

struct NodeBatchInsertLocalState final : public BatchInsertLocalState {
    std::optional<IndexBuilder> localIndexBuilder;

    common::DataChunkState* columnState;
    std::vector<std::shared_ptr<common::ValueVector>> nullColumnVectors;
    std::vector<common::ValueVector*> columnVectors;
};

class NodeBatchInsert final : public BatchInsert {
public:
    NodeBatchInsert(std::unique_ptr<BatchInsertInfo> info,
        std::shared_ptr<BatchInsertSharedState> sharedState,
        std::unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : BatchInsert{std::move(info), std::move(sharedState), std::move(resultSetDescriptor), id,
              paramsString} {
        children.push_back(std::move(child));
    }

    inline bool canParallel() const override {
        auto nodeInfo = common::ku_dynamic_cast<BatchInsertInfo*, NodeBatchInsertInfo*>(info.get());
        return !nodeInfo->containSerial;
    }

    void initGlobalStateInternal(ExecutionContext* context) override;

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    void executeInternal(ExecutionContext* context) override;

    void finalize(ExecutionContext* context) override;

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<NodeBatchInsert>(info->copy(), sharedState,
            resultSetDescriptor->copy(), children[0]->clone(), id, paramsString);
    }

    static void writeAndResetNewNodeGroup(common::node_group_idx_t nodeGroupIdx,
        std::optional<IndexBuilder>& indexBuilder, common::column_id_t pkColumnID,
        storage::NodeTable* table, storage::ChunkedNodeGroup* nodeGroup);

    // The node group will be reset so that the only values remaining are the ones which were not
    // written
    void writeAndResetNodeGroup(common::node_group_idx_t nodeGroupIdx, ExecutionContext* context,
        std::unique_ptr<storage::ChunkedNodeGroup>& nodeGroup,
        std::optional<IndexBuilder>& indexBuilder);

private:
    // Returns the number of nodes written from the group
    uint64_t writeToExistingNodeGroup(transaction::Transaction* transaction,
        common::node_group_idx_t nodeGroupIdx, std::optional<IndexBuilder>& indexBuilder,
        common::column_id_t pkColumnID, storage::NodeTable* table,
        storage::ChunkedNodeGroup* nodeGroup);

    void appendIncompleteNodeGroup(std::unique_ptr<storage::ChunkedNodeGroup> localNodeGroup,
        std::optional<IndexBuilder>& indexBuilder, ExecutionContext* context);
    void clearToIndex(std::unique_ptr<storage::ChunkedNodeGroup>& nodeGroup,
        common::offset_t startIndexInGroup);

    void copyToNodeGroup(ExecutionContext* context);
};

} // namespace processor
} // namespace kuzu
