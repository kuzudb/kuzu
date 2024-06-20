#pragma once

#include "common/copier_config/hnsw_reader_config.h"
#include "processor/operator/sink.h"
#include "storage/store/chunked_node_group_collection.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

struct BulkVectorIndexingSharedState {
    // TODO
};

struct BulkVectorIndexingLocalState {
    DataPos offsetPos;
    DataPos embeddingPos;
    table_id_t tableID;
    property_id_t propertyID;
    HnswReaderConfig hnswReaderConfig;
    ValueVector* offsetVector = nullptr;
    ValueVector* embeddingVector = nullptr;

    BulkVectorIndexingLocalState(DataPos offsetPos, DataPos embeddingPos, table_id_t tableID,
        property_id_t propertyID, HnswReaderConfig hnswReaderConfig)
        : offsetPos{offsetPos}, embeddingPos{embeddingPos}, tableID{tableID},
          propertyID{propertyID}, hnswReaderConfig{hnswReaderConfig} {}

    inline std::unique_ptr<BulkVectorIndexingLocalState> copy() {
        return std::make_unique<BulkVectorIndexingLocalState>(offsetPos, embeddingPos, tableID,
            propertyID, hnswReaderConfig);
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

    std::unique_ptr<PhysicalOperator> clone() final;

private:
    std::unique_ptr<BulkVectorIndexingLocalState> localState;
    std::shared_ptr<BulkVectorIndexingSharedState> sharedState;
};

} // namespace processor
} // namespace kuzu
