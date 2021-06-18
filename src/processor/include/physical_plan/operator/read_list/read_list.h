#pragma once

#include "src/common/include/vector/node_vector.h"
#include "src/processor/include/physical_plan/operator/physical_operator.h"
#include "src/storage/include/data_structure/lists/lists.h"

namespace graphflow {
namespace processor {

class ReadList : public PhysicalOperator {

public:
    ReadList(const uint64_t& inDataChunkPos, const uint64_t& inValueVectorPos, BaseLists* lists,
        unique_ptr<PhysicalOperator> prevOperator, ExecutionContext& context, uint32_t id);

    ~ReadList() { lists->reclaim(handle); }

    void printMetricsToJson(nlohmann::json& json, Profiler& profiler) override;

protected:
    void readValuesFromList();

protected:
    static constexpr uint32_t MAX_TO_READ = 512;

    uint64_t inDataChunkPos;
    uint64_t inValueVectorPos;
    shared_ptr<DataChunk> inDataChunk;
    shared_ptr<NodeIDVector> inNodeIDVector;
    shared_ptr<DataChunk> outDataChunk;
    shared_ptr<ValueVector> outValueVector;

    BaseLists* lists;
    unique_ptr<DataStructureHandle> handle;
};

} // namespace processor
} // namespace graphflow
