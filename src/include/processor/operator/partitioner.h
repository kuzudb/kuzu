#pragma once

#include "common/enums/column_evaluate_type.h"
#include "expression_evaluator/expression_evaluator.h"
#include "processor/operator/sink.h"
#include "storage/store/in_mem_chunked_node_group_collection.h"

namespace kuzu {
namespace storage {
class NodeTable;
class RelTable;
class MemoryManager;
} // namespace storage
namespace transaction {
class Transaction;
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
    std::vector<std::unique_ptr<storage::InMemChunkedNodeGroupCollection>> partitions;

    void merge(std::unique_ptr<PartitioningBuffer> localPartitioningStates) const;
};

// NOTE: Currently, Partitioner is tightly coupled with RelBatchInsert. We should generalize it
// later when necessary. Here, each partition is essentially a node group.
struct BatchInsertSharedState;
struct PartitioningInfo;
struct PartitionerDataInfo;
struct RelBatchInsertProgressSharedState;
struct PartitionerSharedState {
    std::mutex mtx;
    storage::NodeTable* srcNodeTable;
    storage::NodeTable* dstNodeTable;
    storage::RelTable* relTable;
    storage::MemoryManager& mm;

    explicit PartitionerSharedState(storage::MemoryManager& mm)
        : mtx{}, srcNodeTable{nullptr}, dstNodeTable{nullptr}, relTable(nullptr), mm{mm},
          maxNodeOffsets{0, 0}, numPartitions{0, 0}, nextPartitionIdx{0} {}

    static constexpr size_t DIRECTIONS = 2;
    // FIXME(Guodong): we should not maintain maxNodeOffsets.
    std::array<common::offset_t, DIRECTIONS> maxNodeOffsets; // max node offset in each direction.
    std::array<common::partition_idx_t, DIRECTIONS>
        numPartitions; // num of partitions in each direction.
    std::vector<std::unique_ptr<PartitioningBuffer>> partitioningBuffers;
    std::atomic<common::partition_idx_t> nextPartitionIdx;

    void initialize(const PartitionerDataInfo& dataInfo, main::ClientContext* clientContext);

    common::partition_idx_t getNextPartition(common::idx_t partitioningIdx,
        RelBatchInsertProgressSharedState& progressSharedState);

    void resetState();
    void merge(std::vector<std::unique_ptr<PartitioningBuffer>> localPartitioningStates);

    // Must only be called once for any given parameters.
    // The data gets moved out of the shared state since some of it may be spilled to disk and will
    // need to be freed after its processed.
    std::unique_ptr<storage::InMemChunkedNodeGroupCollection> getPartitionBuffer(
        common::idx_t partitioningIdx, common::partition_idx_t partitionIdx) const {
        KU_ASSERT(partitioningIdx < partitioningBuffers.size());
        KU_ASSERT(partitionIdx < partitioningBuffers[partitioningIdx]->partitions.size());

        KU_ASSERT(partitioningBuffers[partitioningIdx]->partitions[partitionIdx].get());
        auto partitioningBuffer =
            std::move(partitioningBuffers[partitioningIdx]->partitions[partitionIdx]);
        // This may still run out of memory if there isn't enough space for one partitioningBuffer
        // per thread
        partitioningBuffer->loadFromDisk(mm);
        return partitioningBuffer;
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
    common::idx_t keyIdx;
    partitioner_func_t partitionerFunc;

    PartitioningInfo(common::idx_t keyIdx, partitioner_func_t partitionerFunc)
        : keyIdx{keyIdx}, partitionerFunc{std::move(partitionerFunc)} {}
    EXPLICIT_COPY_DEFAULT_MOVE(PartitioningInfo);

private:
    PartitioningInfo(const PartitioningInfo& other)
        : keyIdx{other.keyIdx}, partitionerFunc{other.partitionerFunc} {}
};

struct PartitionerDataInfo {
    std::vector<common::LogicalType> columnTypes;
    evaluator::evaluator_vector_t columnEvaluators;
    std::vector<common::ColumnEvaluateType> evaluateTypes;

    PartitionerDataInfo(std::vector<common::LogicalType> columnTypes,
        std::vector<std::unique_ptr<evaluator::ExpressionEvaluator>> columnEvaluators,
        std::vector<common::ColumnEvaluateType> evaluateTypes)
        : columnTypes{std::move(columnTypes)}, columnEvaluators{std::move(columnEvaluators)},
          evaluateTypes{std::move(evaluateTypes)} {}
    EXPLICIT_COPY_DEFAULT_MOVE(PartitionerDataInfo);

private:
    PartitionerDataInfo(const PartitionerDataInfo& other)
        : columnTypes{common::LogicalType::copy(other.columnTypes)},
          columnEvaluators{cloneVector(other.columnEvaluators)},
          evaluateTypes{other.evaluateTypes} {}
};

struct PartitionerInfo {
    DataPos relOffsetDataPos;
    std::vector<PartitioningInfo> infos;

    PartitionerInfo() {}
    PartitionerInfo(const PartitionerInfo& other) : relOffsetDataPos{other.relOffsetDataPos} {
        infos.reserve(other.infos.size());
        for (auto& otherInfo : other.infos) {
            infos.push_back(otherInfo.copy());
        }
    }

    EXPLICIT_COPY_METHOD(PartitionerInfo);
};

struct PartitionerPrintInfo final : OPPrintInfo {
    binder::expression_vector expressions;

    explicit PartitionerPrintInfo(binder::expression_vector expressions)
        : expressions{std::move(expressions)} {}

    std::string toString() const override;

    std::unique_ptr<OPPrintInfo> copy() const override {
        return std::unique_ptr<PartitionerPrintInfo>(new PartitionerPrintInfo(*this));
    }

private:
    PartitionerPrintInfo(const PartitionerPrintInfo& other)
        : OPPrintInfo{other}, expressions{other.expressions} {}
};

class Partitioner final : public Sink {
public:
    Partitioner(std::unique_ptr<ResultSetDescriptor> resultSetDescriptor, PartitionerInfo info,
        PartitionerDataInfo dataInfo, std::shared_ptr<PartitionerSharedState> sharedState,
        std::unique_ptr<PhysicalOperator> child, physical_op_id id,
        std::unique_ptr<OPPrintInfo> printInfo);

    void initGlobalStateInternal(ExecutionContext* context) override;
    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;
    void executeInternal(ExecutionContext* context) override;

    std::shared_ptr<PartitionerSharedState> getSharedState() { return sharedState; }

    std::unique_ptr<PhysicalOperator> clone() override;

    static void initializePartitioningStates(const PartitionerDataInfo& dataInfo,
        std::vector<std::unique_ptr<PartitioningBuffer>>& partitioningBuffers,
        const std::array<common::partition_idx_t, PartitionerSharedState::DIRECTIONS>&
            numPartitions);

private:
    common::DataChunk constructDataChunk(
        const std::shared_ptr<common::DataChunkState>& state) const;
    // TODO: For now, RelBatchInsert will guarantee all data are inside one data chunk. Should be
    //  generalized to resultSet later if needed.
    void copyDataToPartitions(storage::MemoryManager& memoryManager,
        common::partition_idx_t partitioningIdx, common::DataChunk chunkToCopyFrom) const;

private:
    PartitionerDataInfo dataInfo;
    PartitionerInfo info;
    std::shared_ptr<PartitionerSharedState> sharedState;
    std::unique_ptr<PartitionerLocalState> localState;

    // Intermediate temp value vector.
    std::unique_ptr<common::ValueVector> partitionIdxes;
};

} // namespace processor
} // namespace kuzu
