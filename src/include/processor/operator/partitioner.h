#pragma once

#include "processor/operator/sink.h"

namespace kuzu {
namespace processor {

using partitioner_func_t =
    std::function<void(common::ValueVector* key, common::ValueVector* result)>;
using data_partition_t = std::vector<std::unique_ptr<common::DataChunk>>;

struct PartitionerFunctions {
    static void partitionRelData(common::ValueVector* key, common::ValueVector* partitionIdxes);
};

// Partitioner operator can duplicate and partition the same data chunk from child with multiple
// partitioning methods. For example, copy of rel tables require partitioning on both FWD and BWD
// direction. Each partitioning method corresponds to a PartitioningState.
struct PartitioningBuffer {
    std::vector<std::unique_ptr<data_partition_t>> partitions;

    void merge(std::unique_ptr<PartitioningBuffer> localPartitioningStates);
};

// NOTE: Currently, Partitioner is tightly coupled with CopyRel. We should generalize it later when
// necessary. Here, each partition is essentially a node group.
struct PartitionerSharedState {
    std::mutex mtx;
    std::vector<common::offset_t> maxNodeOffsets;       // max node offset in each direction.
    std::vector<common::partition_idx_t> numPartitions; // num of partitions in each direction.
    std::vector<std::unique_ptr<PartitioningBuffer>> partitioningBuffers;
    common::partition_idx_t nextPartitionIdx = 0;

    void initialize();
    common::partition_idx_t getNextPartition(common::vector_idx_t partitioningIdx);
    void resetState();
    void merge(std::vector<std::unique_ptr<PartitioningBuffer>> localPartitioningStates);
};

struct PartitionerLocalState {
    std::vector<std::unique_ptr<PartitioningBuffer>> partitioningBuffers;

    PartitioningBuffer* getPartitioningBuffer(common::partition_idx_t partitioningIdx) {
        KU_ASSERT(partitioningIdx < partitioningBuffers.size());
        return partitioningBuffers[partitioningIdx].get();
    }
};

struct PartitioningInfo {
    DataPos keyDataPos;
    std::vector<DataPos> columnDataPositions;
    common::logical_types_t columnTypes;
    partitioner_func_t partitionerFunc;

    PartitioningInfo(DataPos keyDataPos, std::vector<DataPos> columnDataPositions,
        common::logical_types_t columnTypes, partitioner_func_t partitionerFunc)
        : keyDataPos{keyDataPos}, columnDataPositions{std::move(columnDataPositions)},
          columnTypes{std::move(columnTypes)}, partitionerFunc{partitionerFunc} {}
    inline std::unique_ptr<PartitioningInfo> copy() {
        return std::make_unique<PartitioningInfo>(keyDataPos, columnDataPositions,
            common::LogicalType::copy(columnTypes), partitionerFunc);
    }

    static std::vector<std::unique_ptr<PartitioningInfo>> copy(
        const std::vector<std::unique_ptr<PartitioningInfo>>& other);
};

class Partitioner : public Sink {
public:
    Partitioner(std::unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        std::vector<std::unique_ptr<PartitioningInfo>> infos,
        std::shared_ptr<PartitionerSharedState> sharedState,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString);

    void initGlobalStateInternal(ExecutionContext* context) final;
    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) final;
    void executeInternal(ExecutionContext* context) final;

    inline std::shared_ptr<PartitionerSharedState> getSharedState() { return sharedState; }

    std::unique_ptr<PhysicalOperator> clone() final;

    static void initializePartitioningStates(
        std::vector<std::unique_ptr<PartitioningBuffer>>& partitioningBuffers,
        std::vector<common::partition_idx_t> numPartitions);

private:
    // TODO: For now, CopyRel will guarantee all data are inside one data chunk. Should be
    //  generalized to resultSet later if needed.
    void copyDataToPartitions(common::partition_idx_t partitioningIdx,
        common::DataChunk* chunkToCopyFrom, storage::MemoryManager* memoryManager);

private:
    std::vector<std::unique_ptr<PartitioningInfo>> infos;
    std::shared_ptr<PartitionerSharedState> sharedState;
    std::unique_ptr<PartitionerLocalState> localState;

    // Intermediate temp value vector.
    std::unique_ptr<common::ValueVector> partitionIdxes;
};

} // namespace processor
} // namespace kuzu
