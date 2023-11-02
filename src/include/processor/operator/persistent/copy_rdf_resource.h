#pragma once

#include "copy_node.h"
#include "processor/operator/sink.h"

namespace kuzu {
namespace processor {

// TODO: This operator should be merged with CopyNode INSERT ON CONFLICT DO NOTHING
class CopyRdfResource : public Sink {
public:
    CopyRdfResource(std::shared_ptr<CopyNodeSharedState> sharedState,
        std::unique_ptr<CopyNodeInfo> info,
        std::unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : Sink{std::move(resultSetDescriptor), PhysicalOperatorType::COPY_RDF, std::move(child), id,
              paramsString},
          sharedState{std::move(sharedState)}, info{std::move(info)} {}

    void initGlobalStateInternal(ExecutionContext* context) final;

    void initLocalStateInternal(ResultSet* resultSet_, ExecutionContext* context) final;

    void executeInternal(kuzu::processor::ExecutionContext* context) final;

    void finalize(ExecutionContext* context) final;

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<CopyRdfResource>(sharedState, info->copy(),
            resultSetDescriptor->copy(), children[0]->clone(), id, paramsString);
    }

private:
    void insertIndex(common::ValueVector* vector);
    void copyToNodeGroup(common::ValueVector* vector);

private:
    std::shared_ptr<CopyNodeSharedState> sharedState;
    std::unique_ptr<CopyNodeInfo> info;

    common::DataChunkState* columnState;
    common::ValueVector* vector;

    std::unique_ptr<storage::NodeGroup> localNodeGroup;
};

} // namespace processor
} // namespace kuzu
