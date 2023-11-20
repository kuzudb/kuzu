#pragma once

#include "processor/operator/call/in_query_call.h"
#include "processor/operator/sink.h"
#include "storage/store/node_group.h"
#include "storage/store/node_table.h"
#include "storage/store/rel_table.h"

namespace kuzu {
namespace processor {

class CopyNodeSharedState {
    friend class PlanMapper;
    friend class CopyNode;
    friend class CopyRdfResource;

public:
    CopyNodeSharedState(InQueryCallSharedState* readerSharedState)
        : readerSharedState{readerSharedState}, pkIndex{nullptr}, currentNodeGroupIdx{0},
          sharedNodeGroup{nullptr} {};

    void init();

    inline common::offset_t getNextNodeGroupIdx() {
        std::unique_lock<std::mutex> lck{mtx};
        return getNextNodeGroupIdxWithoutLock();
    }

    inline uint64_t getCurNodeGroupIdx() const { return currentNodeGroupIdx; }

    void appendLocalNodeGroup(std::unique_ptr<storage::NodeGroup> localNodeGroup);

private:
    inline common::offset_t getNextNodeGroupIdxWithoutLock() { return currentNodeGroupIdx++; }

private:
    std::mutex mtx;
    storage::WAL* wal;
    storage::NodeTable* table;
    std::vector<std::unique_ptr<common::LogicalType>> columnTypes;
    // Primary key info
    common::vector_idx_t pkColumnIdx;
    std::unique_ptr<common::LogicalType> pkType;
    std::unique_ptr<storage::PrimaryKeyIndexBuilder> pkIndex;

    InQueryCallSharedState* readerSharedState;
    // Table storing result message.
    std::shared_ptr<FactorizedTable> fTable;

    uint64_t currentNodeGroupIdx;
    // The sharedNodeGroup is to accumulate left data within local node groups in CopyNode ops.
    std::unique_ptr<storage::NodeGroup> sharedNodeGroup;
};

struct CopyNodeInfo {
    std::vector<DataPos> columnPositions;
    storage::NodeTable* table;
    std::unordered_set<storage::RelTable*> fwdRelTables;
    std::unordered_set<storage::RelTable*> bwdRelTables;
    std::string tableName;
    bool containsSerial;
    bool compressionEnabled;

    CopyNodeInfo(std::vector<DataPos> columnPositions, storage::NodeTable* table,
        std::unordered_set<storage::RelTable*> fwdRelTables,
        std::unordered_set<storage::RelTable*> bwdRelTables, std::string tableName,
        bool containsSerial, bool compressionEnabled)
        : columnPositions{std::move(columnPositions)}, table{table}, fwdRelTables{fwdRelTables},
          bwdRelTables{bwdRelTables}, tableName{std::move(tableName)},
          containsSerial{containsSerial}, compressionEnabled{compressionEnabled} {}
    CopyNodeInfo(const CopyNodeInfo& other) = default;

    inline std::unique_ptr<CopyNodeInfo> copy() const {
        return std::make_unique<CopyNodeInfo>(*this);
    }
};

class CopyNode : public Sink {
public:
    CopyNode(std::shared_ptr<CopyNodeSharedState> sharedState, std::unique_ptr<CopyNodeInfo> info,
        std::unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : Sink{std::move(resultSetDescriptor), PhysicalOperatorType::COPY_NODE, std::move(child),
              id, paramsString},
          sharedState{std::move(sharedState)}, info{std::move(info)} {}

    inline bool canParallel() const final { return !info->containsSerial; }

    void initGlobalStateInternal(ExecutionContext* context) final;

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) final;

    void executeInternal(ExecutionContext* context) override;

    void finalize(ExecutionContext* context) override;

    inline std::unique_ptr<PhysicalOperator> clone() final {
        return std::make_unique<CopyNode>(sharedState, info->copy(), resultSetDescriptor->copy(),
            children[0]->clone(), id, paramsString);
    }

    static void writeAndResetNodeGroup(common::node_group_idx_t nodeGroupIdx,
        storage::PrimaryKeyIndexBuilder* pkIndex, common::column_id_t pkColumnID,
        storage::NodeTable* table, storage::NodeGroup* nodeGroup);

private:
    static void populatePKIndex(storage::PrimaryKeyIndexBuilder* pkIndex,
        storage::ColumnChunk* chunk, common::offset_t startNodeOffset, common::offset_t numNodes);
    static void checkNonNullConstraint(
        storage::NullColumnChunk* nullChunk, common::offset_t numNodes);

    template<typename T>
    static uint64_t appendToPKIndex(storage::PrimaryKeyIndexBuilder* pkIndex,
        storage::ColumnChunk* chunk, common::offset_t startOffset, common::offset_t numNodes);

    void copyToNodeGroup();

protected:
    std::shared_ptr<CopyNodeSharedState> sharedState;
    std::unique_ptr<CopyNodeInfo> info;

    common::DataChunkState* columnState;
    std::vector<std::shared_ptr<common::ValueVector>> nullColumnVectors;
    std::vector<common::ValueVector*> columnVectors;
    std::unique_ptr<storage::NodeGroup> localNodeGroup;
};

template<>
uint64_t CopyNode::appendToPKIndex<int64_t>(storage::PrimaryKeyIndexBuilder* pkIndex,
    storage::ColumnChunk* chunk, common::offset_t startOffset, common::offset_t numNodes);
template<>
uint64_t CopyNode::appendToPKIndex<std::string>(storage::PrimaryKeyIndexBuilder* pkIndex,
    storage::ColumnChunk* chunk, common::offset_t startOffset, common::offset_t numNodes);

} // namespace processor
} // namespace kuzu
