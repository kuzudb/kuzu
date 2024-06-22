#pragma once

#include "processor/operator/sink.h"
#include "storage/store/chunked_node_group_collection.h"

namespace kuzu {
namespace storage {
class NodeTable;
class RelTable;
} // namespace storage

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
    std::vector<std::unique_ptr<storage::ChunkedNodeGroupCollection>> partitions;

    void merge(std::unique_ptr<PartitioningBuffer> localPartitioningStates);
};

// NOTE: Currently, Partitioner is tightly coupled with RelBatchInsert. We should generalize it
// later when necessary. Here, each partition is essentially a node group.
struct BatchInsertSharedState;
struct PartitioningInfo;
struct PartitionerSharedState {
    std::vector<common::logical_type_vec_t> columnTypes;
    std::mutex mtx;
    storage::NodeTable* srcNodeTable;
    storage::NodeTable* dstNodeTable;
    storage::RelTable* relTable;

    // FIXME(Guodong): we should not maintain maxNodeOffsets.
    std::vector<common::offset_t> maxNodeOffsets;       // max node offset in each direction.
    std::vector<common::partition_idx_t> numPartitions; // num of partitions in each direction.
    std::vector<std::unique_ptr<PartitioningBuffer>> partitioningBuffers;
    common::partition_idx_t nextPartitionIdx = 0;
    // In copy rdf, we need to access num nodes before it is available in statistics.
    std::vector<std::shared_ptr<BatchInsertSharedState>> nodeBatchInsertSharedStates;

    explicit PartitionerSharedState(std::vector<common::logical_type_vec_t> columnTypes)
        : columnTypes{std::move(columnTypes)}, srcNodeTable{nullptr}, dstNodeTable{nullptr},
          relTable{nullptr} {}

    void initialize(const std::vector<std::unique_ptr<PartitioningInfo>>& infos);
    common::partition_idx_t getNextPartition(common::idx_t partitioningIdx);
    void resetState();
    void merge(std::vector<std::unique_ptr<PartitioningBuffer>> localPartitioningStates);

    storage::ChunkedNodeGroupCollection& getPartitionBuffer(common::idx_t partitioningIdx,
        common::partition_idx_t partitionIdx) const {
        KU_ASSERT(partitioningIdx < partitioningBuffers.size());
        KU_ASSERT(partitionIdx < partitioningBuffers[partitioningIdx]->partitions.size());
        return *partitioningBuffers[partitioningIdx]->partitions[partitionIdx];
    }
};

struct PartitionerLocalState {
    std::vector<std::unique_ptr<PartitioningBuffer>> partitioningBuffers;

    PartitioningBuffer* getPartitioningBuffer(common::partition_idx_t partitioningIdx) const {
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
    std::unique_ptr<PartitioningInfo> copy() {
        return std::make_unique<PartitioningInfo>(keyDataPos, columnDataPositions,
            common::LogicalType::copy(columnTypes), partitionerFunc);
    }
};

struct PartitionerInfo {
    DataPos relOffsetDataPos;
    std::vector<std::unique_ptr<PartitioningInfo>> infos;

    PartitionerInfo() {}
    PartitionerInfo(const PartitionerInfo& other) : relOffsetDataPos{other.relOffsetDataPos} {
        infos.reserve(other.infos.size());
        for (auto& otherInfo : other.infos) {
            infos.push_back(otherInfo->copy());
        }
    }

    EXPLICIT_COPY_METHOD(PartitionerInfo);
};

class Partitioner final : public Sink {
public:
    Partitioner(std::unique_ptr<ResultSetDescriptor> resultSetDescriptor, PartitionerInfo info,
        std::shared_ptr<PartitionerSharedState> sharedState,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString);

    void initGlobalStateInternal(ExecutionContext* context) override;
    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;
    void executeInternal(ExecutionContext* context) override;

    std::shared_ptr<PartitionerSharedState> getSharedState() { return sharedState; }

    std::unique_ptr<PhysicalOperator> clone() override;

    static void initializePartitioningStates(
        const std::vector<std::unique_ptr<PartitioningInfo>>& infos,
        std::vector<std::unique_ptr<PartitioningBuffer>>& partitioningBuffers,
        const std::vector<common::partition_idx_t>& numPartitions);

private:
    common::DataChunk constructDataChunk(const std::vector<DataPos>& columnPositions,
        const std::vector<common::LogicalType>& columnTypes, const ResultSet& resultSet,
        const std::shared_ptr<common::DataChunkState>& state);
    // TODO: For now, RelBatchInsert will guarantee all data are inside one data chunk. Should be
    //  generalized to resultSet later if needed.
    void copyDataToPartitions(common::partition_idx_t partitioningIdx,
        common::DataChunk chunkToCopyFrom) const;

private:
    PartitionerInfo info;
    std::shared_ptr<PartitionerSharedState> sharedState;
    std::unique_ptr<PartitionerLocalState> localState;

    // Intermediate temp value vector.
    std::unique_ptr<common::ValueVector> partitionIdxes;
};

} // namespace processor
} // namespace kuzu
