#pragma once

#include "common/vector_index/vector_index_config.h"
#include "processor/operator/partitioner.h"
#include "processor/operator/sink.h"
#include "storage/index/vector_index_builder.h"
#include "storage/index/vector_index_header.h"
#include "storage/store/chunked_node_group_collection.h"
#include "storage/store/node_table.h"
#include <latch>

using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {
    struct BulkVectorIndexingSharedState {
        VectorIndexHeader *header;
        int partitionId;
        VectorIndexHeaderPerPartition *headerPerPartition;
        std::shared_ptr<PartitionerSharedState> partitionerSharedState;

        std::unique_ptr<VectorIndexBuilder> builder;
        std::unique_ptr<VectorIndexGraph> graph;
//        std::unique_ptr<VectorTempStorage> tempStorage;
        std::unique_ptr<CompressedVectorStorage> compressedStorage;
        Column *compressedPropertyColumn;
        std::latch compressionLatch;
        offset_t startOffsetNodeTable;
        offset_t numVectors;

        explicit BulkVectorIndexingSharedState(VectorIndexHeader *header, int partitionId,
                                               std::shared_ptr<PartitionerSharedState> partitionerSharedState,
                                               int numThreads)
                : header{header}, partitionId{partitionId},
                  partitionerSharedState{partitionerSharedState}, compressionLatch(numThreads) {}
    };

struct BulkVectorIndexingLocalState {
    DataPos offsetPos;
    DataPos embeddingPos;
    std::unique_ptr<NodeTableDistanceComputer<float>> dc;
    std::unique_ptr<VisitedTable> visited;
    std::unique_ptr<Quantizer> quantizer;
    ValueVector* offsetVector = nullptr;
    ValueVector* embeddingVector = nullptr;

    explicit BulkVectorIndexingLocalState(DataPos offsetPos, DataPos embeddingPos)
        : offsetPos{offsetPos}, embeddingPos{embeddingPos} {}

    inline std::unique_ptr<BulkVectorIndexingLocalState> copy() {
        return std::make_unique<BulkVectorIndexingLocalState>(offsetPos, embeddingPos);
    }
};

class BulkVectorIndexing : public Sink {
public:
    explicit BulkVectorIndexing(std::unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        std::unique_ptr<BulkVectorIndexingLocalState> localState,
        std::shared_ptr<BulkVectorIndexingSharedState> sharedState,
        std::unique_ptr<PhysicalOperator> child, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo);

    void initGlobalStateInternal(ExecutionContext* context) final;

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) final;

    void executeInternal(ExecutionContext* context) final;

    inline void resetScanTable(transaction::Transaction* transaction);

    void finalize(ExecutionContext* context) override;

    std::unique_ptr<PhysicalOperator> clone() final;

private:
    std::unique_ptr<BulkVectorIndexingLocalState> localState;
    std::shared_ptr<BulkVectorIndexingSharedState> sharedState;
    bool initialized = false;
};

} // namespace processor
} // namespace kuzu
