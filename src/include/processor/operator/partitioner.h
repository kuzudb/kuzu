#pragma once

#include "processor/operator/sink.h"
#include "storage/store/column_chunk.h"

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
    using ColumnChunkCollection = std::vector<std::unique_ptr<storage::ColumnChunk>>;
    struct Partition {
        // One chunk for each column, grouped into a list
        // so that groups from different threads can be quickly merged without copying
        // E.g. [(a,b,c), (a,b,c)] where a is a chunk for column a, b for column b, etc.
        std::vector<ColumnChunkCollection> chunks;
    };
    std::vector<Partition> partitions;

    void merge(std::unique_ptr<PartitioningBuffer> localPartitioningStates);
};

// NOTE: Currently, Partitioner is tightly coupled with RelBatchInsert. We should generalize it
// later when necessary. Here, each partition is essentially a node group.
struct BatchInsertSharedState;
struct PartitionerSharedState {
    std::mutex mtx;
    storage::MemoryManager* mm;
    storage::NodeTable* srcNodeTable;
    storage::NodeTable* dstNodeTable;

    // FIXME(Guodong): we should not maintain maxNodeOffsets.
    std::vector<common::offset_t> maxNodeOffsets;       // max node offset in each direction.
    std::vector<common::partition_idx_t> numPartitions; // num of partitions in each direction.
    std::vector<std::unique_ptr<PartitioningBuffer>> partitioningBuffers;
    common::partition_idx_t nextPartitionIdx = 0;
    // In copy rdf, we need to access num nodes before it is available in statistics.
    std::vector<std::shared_ptr<BatchInsertSharedState>> nodeBatchInsertSharedStates;

    explicit PartitionerSharedState(storage::MemoryManager* mm) : mm{mm} {}

    void initialize();
    common::partition_idx_t getNextPartition(common::vector_idx_t partitioningIdx);
    void resetState();
    void merge(std::vector<std::unique_ptr<PartitioningBuffer>> localPartitioningStates);

    inline PartitioningBuffer::Partition& getPartitionBuffer(
        common::vector_idx_t partitioningIdx, common::partition_idx_t partitionIdx) {
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
    DataPos keyDataPos;
    std::vector<DataPos> columnDataPositions;
    std::vector<common::LogicalType> columnTypes;
    partitioner_func_t partitionerFunc;

    PartitioningInfo(DataPos keyDataPos, std::vector<DataPos> columnDataPositions,
        std::vector<common::LogicalType> columnTypes, partitioner_func_t partitionerFunc)
        : keyDataPos{keyDataPos}, columnDataPositions{std::move(columnDataPositions)},
          columnTypes{std::move(columnTypes)}, partitionerFunc{std::move(partitionerFunc)} {}
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
    common::DataChunk constructDataChunk(const std::vector<DataPos>& columnPositions,
        const std::vector<common::LogicalType>& columnTypes, const ResultSet& resultSet,
        const std::shared_ptr<common::DataChunkState>& state);
    // TODO: For now, RelBatchInsert will guarantee all data are inside one data chunk. Should be
    //  generalized to resultSet later if needed.
    void copyDataToPartitions(
        common::partition_idx_t partitioningIdx, common::DataChunk chunkToCopyFrom);

private:
    // Same size as a value vector. Each thread will allocate a chunk for each node group,
    // so this should be kept relatively small to avoid allocating more memory than is needed
    static const uint64_t CHUNK_SIZE = 2048;
    std::vector<std::unique_ptr<PartitioningInfo>> infos;
    std::shared_ptr<PartitionerSharedState> sharedState;
    std::unique_ptr<PartitionerLocalState> localState;

    // Intermediate temp value vector.
    std::unique_ptr<common::ValueVector> partitionIdxes;
};

} // namespace processor
} // namespace kuzu
