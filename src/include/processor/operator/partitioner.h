#pragma once

#include "expression_evaluator/expression_evaluator.h"
#include "processor/operator/sink.h"
#include "storage/store/chunked_node_group_collection.h"

namespace kuzu {
namespace storage {
class NodeTable;
}
namespace processor {

using partitioner_func_t =
    std::function<void(common::ValueVector* key, common::ValueVector* result)>;

struct PartitionerFunctions {
    static void partitionRelData(common::ValueVector* key, common::ValueVector* partitionIdxes);
};

// Partitioner operator can duplicate and partition the same data chunk from child with multiple
// partitioning methods. For example, copy of rel tables require partitioning on both FWD and BWD
// direction. Each partitioning method corresponds to a PartitioningState.
struct PartitioningBuffer {
    std::vector<storage::ChunkedNodeGroupCollection> partitions;

    void merge(std::unique_ptr<PartitioningBuffer> localPartitioningStates);
};

// NOTE: Currently, Partitioner is tightly coupled with RelBatchInsert. We should generalize it
// later when necessary. Here, each partition is essentially a node group.
struct BatchInsertSharedState;
struct PartitioningInfo;
struct PartitionerDataInfo;
struct PartitionerSharedState {
    std::mutex mtx;
    storage::NodeTable* srcNodeTable;
    storage::NodeTable* dstNodeTable;

    // FIXME(Guodong): we should not maintain maxNodeOffsets.
    std::vector<common::offset_t> maxNodeOffsets;       // max node offset in each direction.
    std::vector<common::partition_idx_t> numPartitions; // num of partitions in each direction.
    std::vector<std::unique_ptr<PartitioningBuffer>> partitioningBuffers;
    common::partition_idx_t nextPartitionIdx = 0;
    // In copy rdf, we need to access num nodes before it is available in statistics.
    std::vector<std::shared_ptr<BatchInsertSharedState>> nodeBatchInsertSharedStates;

    void initialize(PartitionerDataInfo& dataInfo);
    common::partition_idx_t getNextPartition(common::idx_t partitioningIdx);
    void resetState();
    void merge(std::vector<std::unique_ptr<PartitioningBuffer>> localPartitioningStates);

    inline const storage::ChunkedNodeGroupCollection& getPartitionBuffer(
        common::idx_t partitioningIdx, common::partition_idx_t partitionIdx) const {
        KU_ASSERT(partitioningIdx < partitioningBuffers.size());
        KU_ASSERT(partitionIdx < partitioningBuffers[partitioningIdx]->partitions.size());
        return partitioningBuffers[partitioningIdx]->partitions[partitionIdx];
    }
};

struct PartitionerLocalState {
    std::vector<std::unique_ptr<PartitioningBuffer>> partitioningBuffers;

    PartitioningBuffer* getPartitioningBuffer(common::partition_idx_t partitioningIdx) {
        KU_ASSERT(partitioningIdx < partitioningBuffers.size());
        return partitioningBuffers[partitioningIdx].get();
    }
};

struct PartitioningInfo {
    common::idx_t keyIdx;
    partitioner_func_t partitionerFunc;

    PartitioningInfo(common::idx_t keyIdx, partitioner_func_t partitionerFunc)
        : keyIdx{keyIdx}, partitionerFunc{std::move(partitionerFunc)} {}
    EXPLICIT_COPY_DEFAULT_MOVE(PartitioningInfo);

    static std::vector<PartitioningInfo> copy(const std::vector<PartitioningInfo>& other);

private:
    PartitioningInfo(const PartitioningInfo& other)
        : keyIdx{other.keyIdx}, partitionerFunc{other.partitionerFunc} {}
};

struct PartitionerDataInfo {
    std::vector<common::LogicalType> columnTypes;
    std::vector<std::unique_ptr<evaluator::ExpressionEvaluator>> columnEvaluators;
    std::vector<bool> defaultColumns;

    PartitionerDataInfo(std::vector<common::LogicalType> columnTypes,
        std::vector<std::unique_ptr<evaluator::ExpressionEvaluator>> columnEvaluators,
        std::vector<bool> defaultColumns)
        : columnTypes{std::move(columnTypes)}, columnEvaluators{std::move(columnEvaluators)},
          defaultColumns{std::move(defaultColumns)} {}
    EXPLICIT_COPY_DEFAULT_MOVE(PartitionerDataInfo);

private:
    PartitionerDataInfo(const PartitionerDataInfo& other)
        : columnTypes{common::LogicalType::copy(other.columnTypes)},
          columnEvaluators{cloneVector(other.columnEvaluators)},
          defaultColumns{other.defaultColumns} {}
};

class Partitioner : public Sink {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::PARTITIONER;

public:
    Partitioner(std::unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        std::vector<PartitioningInfo> infos, PartitionerDataInfo dataInfo,
        std::shared_ptr<PartitionerSharedState> sharedState,
        std::unique_ptr<PhysicalOperator> child, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo);

    void initGlobalStateInternal(ExecutionContext* context) final;
    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) final;
    void executeInternal(ExecutionContext* context) final;

    std::shared_ptr<PartitionerSharedState> getSharedState() { return sharedState; }

    std::unique_ptr<PhysicalOperator> clone() final;

    static void initializePartitioningStates(PartitionerDataInfo& dataInfo,
        std::vector<std::unique_ptr<PartitioningBuffer>>& partitioningBuffers,
        std::vector<common::partition_idx_t> numPartitions);

private:
    void evaluateData(const common::sel_t& numTuples);
    common::DataChunk constructDataChunk(const std::shared_ptr<common::DataChunkState>& state);
    // TODO: For now, RelBatchInsert will guarantee all data are inside one data chunk. Should be
    //  generalized to resultSet later if needed.
    void copyDataToPartitions(common::partition_idx_t partitioningIdx,
        common::DataChunk chunkToCopyFrom);

private:
    std::vector<PartitioningInfo> infos;
    PartitionerDataInfo dataInfo;
    std::shared_ptr<PartitionerSharedState> sharedState;
    std::unique_ptr<PartitionerLocalState> localState;

    // Intermediate temp value vector.
    std::unique_ptr<common::ValueVector> partitionIdxes;
};

} // namespace processor
} // namespace kuzu
