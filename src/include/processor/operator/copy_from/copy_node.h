#pragma once

#include "common/copier_config/copier_config.h"
#include "processor/operator/sink.h"
#include "storage/copier/node_group.h"
#include "storage/store/node_table.h"

namespace kuzu {
namespace processor {

class CopyNodeSharedState {
public:
    CopyNodeSharedState(uint64_t& numRows, catalog::NodeTableSchema* tableSchema,
        storage::NodeTable* table, const common::CopyDescription& copyDesc,
        storage::MemoryManager* memoryManager);

    inline void initialize(const std::string& directory) { initializePrimaryKey(directory); };

    inline common::offset_t getNextNodeGroupIdx() {
        std::unique_lock<std::mutex> lck{mtx};
        return getNextNodeGroupIdxWithoutLock();
    }

    void logCopyNodeWALRecord(storage::WAL* wal);

    void appendLocalNodeGroup(std::unique_ptr<storage::NodeGroup> localNodeGroup);

private:
    void initializePrimaryKey(const std::string& directory);
    inline common::offset_t getNextNodeGroupIdxWithoutLock() { return currentNodeGroupIdx++; }

public:
    std::mutex mtx;
    common::column_id_t pkColumnID;
    std::unique_ptr<storage::PrimaryKeyIndexBuilder> pkIndex;
    common::CopyDescription copyDesc;
    storage::NodeTable* table;
    catalog::NodeTableSchema* tableSchema;
    uint64_t& numRows;
    std::shared_ptr<FactorizedTable> fTable;
    bool hasLoggedWAL;
    uint64_t currentNodeGroupIdx;
    // The sharedNodeGroup is to accumulate left data within local node groups in CopyNode ops.
    std::unique_ptr<storage::NodeGroup> sharedNodeGroup;
};

struct CopyNodeInfo {
    std::vector<DataPos> dataColumnPoses;
    DataPos nodeOffsetPos;
    common::CopyDescription copyDesc;
    storage::NodeTable* table;
    storage::RelsStore* relsStore;
    catalog::Catalog* catalog;
    storage::WAL* wal;
    bool preservingOrder;
};

class CopyNode : public Sink {
public:
    CopyNode(std::shared_ptr<CopyNodeSharedState> sharedState, CopyNodeInfo copyNodeInfo,
        std::unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString);

    inline void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override {
        for (auto& arrowColumnPos : copyNodeInfo.dataColumnPoses) {
            dataColumnVectors.push_back(resultSet->getValueVector(arrowColumnPos).get());
        }
        nodeOffsetVector = resultSet->getValueVector(copyNodeInfo.nodeOffsetPos).get();
        localNodeGroup =
            std::make_unique<storage::NodeGroup>(sharedState->tableSchema, &sharedState->copyDesc);
    }

    inline void initGlobalStateInternal(ExecutionContext* context) override;

    void executeInternal(ExecutionContext* context) override;

    void finalize(ExecutionContext* context) override;

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<CopyNode>(sharedState, copyNodeInfo, resultSetDescriptor->copy(),
            children[0]->clone(), id, paramsString);
    }

    static void writeAndResetNodeGroup(common::node_group_idx_t nodeGroupIdx,
        storage::PrimaryKeyIndexBuilder* pkIndex, common::column_id_t pkColumnID,
        storage::NodeTable* table, storage::NodeGroup* nodeGroup);

private:
    inline bool isCopyAllowed() const {
        auto nodesStatistics = copyNodeInfo.table->getNodeStatisticsAndDeletedIDs();
        return nodesStatistics->getNodeStatisticsAndDeletedIDs(copyNodeInfo.table->getTableID())
                   ->getNumTuples() == 0;
    }

    static void populatePKIndex(storage::PrimaryKeyIndexBuilder* pkIndex,
        storage::ColumnChunk* chunk, common::offset_t startNodeOffset, common::offset_t numNodes);
    static void checkNonNullConstraint(
        storage::NullColumnChunk* nullChunk, common::offset_t numNodes);

    template<typename T>
    static uint64_t appendToPKIndex(storage::PrimaryKeyIndexBuilder* pkIndex,
        storage::ColumnChunk* chunk, common::offset_t startOffset, common::offset_t numNodes);

private:
    common::ValueVector* nodeOffsetVector;
    std::shared_ptr<CopyNodeSharedState> sharedState;
    CopyNodeInfo copyNodeInfo;
    std::vector<common::ValueVector*> dataColumnVectors;
    std::unique_ptr<storage::NodeGroup> localNodeGroup;
};

template<>
uint64_t CopyNode::appendToPKIndex<int64_t>(storage::PrimaryKeyIndexBuilder* pkIndex,
    storage::ColumnChunk* chunk, common::offset_t startOffset, common::offset_t numNodes);
template<>
uint64_t CopyNode::appendToPKIndex<common::ku_string_t>(storage::PrimaryKeyIndexBuilder* pkIndex,
    storage::ColumnChunk* chunk, common::offset_t startOffset, common::offset_t numNodes);

} // namespace processor
} // namespace kuzu
