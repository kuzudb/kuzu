#pragma once

#include "common/copier_config/copier_config.h"
#include "processor/operator/sink.h"
#include "storage/store/node_group.h"
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

struct CopyNodeLocalState {
    std::unique_ptr<storage::NodeGroup> nodeGroup;
};

struct CopyNodeDataInfo {
    std::vector<DataPos> dataColumnPoses;
};

class CopyNode : public Sink {
public:
    CopyNode(storage::RelsStore* relsStore, storage::WAL* wal,
        std::shared_ptr<CopyNodeSharedState> sharedState, CopyNodeDataInfo dataInfo,
        std::unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : Sink{std::move(resultSetDescriptor), PhysicalOperatorType::COPY_NODE, std::move(child),
              id, paramsString},
          relsStore{relsStore}, wal{wal}, sharedState{std::move(sharedState)}, dataInfo{std::move(
                                                                                   dataInfo)} {}

    inline void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override {
        localState = std::make_unique<CopyNodeLocalState>();
        localState->nodeGroup =
            std::make_unique<storage::NodeGroup>(sharedState->tableSchema, &sharedState->copyDesc);
    }

    void initGlobalStateInternal(ExecutionContext* context) override;

    void executeInternal(ExecutionContext* context) override;

    void finalize(ExecutionContext* context) override;

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<CopyNode>(relsStore, wal, sharedState, dataInfo,
            resultSetDescriptor->copy(), children[0]->clone(), id, paramsString);
    }

    static void appendNodeGroupToTableAndPopulateIndex(storage::NodeTable* table,
        storage::NodeGroup* nodeGroup, storage::PrimaryKeyIndexBuilder* pkIndex,
        common::column_id_t pkColumnID);

private:
    inline bool isCopyAllowed() {
        auto nodesStatistics = sharedState->table->getNodeStatisticsAndDeletedIDs();
        return nodesStatistics->getNodeStatisticsAndDeletedIDs(sharedState->table->getTableID())
                   ->getNumTuples() == 0;
    }

    static std::shared_ptr<common::DataChunk> sliceDataVectorsInDataChunk(
        const common::DataChunk& dataChunkToSlice, const std::vector<DataPos>& dataColumnPoses,
        int64_t offset, int64_t length);

    static void populatePKIndex(storage::PrimaryKeyIndexBuilder* pkIndex,
        storage::ColumnChunk* chunk, common::offset_t startNodeOffset, common::offset_t numNodes);
    static void appendToPKIndex(storage::PrimaryKeyIndexBuilder* pkIndex,
        storage::ColumnChunk* chunk, common::offset_t startOffset, common::offset_t numNodes);

private:
    storage::RelsStore* relsStore;
    storage::WAL* wal;
    CopyNodeDataInfo dataInfo;
    std::unique_ptr<CopyNodeLocalState> localState;
    std::shared_ptr<CopyNodeSharedState> sharedState;
};

} // namespace processor
} // namespace kuzu
