#pragma once

#include "src/processor/include/physical_plan/operator/physical_operator.h"
#include "src/storage/include/data_structure/lists/lists.h"

namespace graphflow {
namespace processor {

class ReadList : public PhysicalOperator {

public:
    ReadList(uint32_t inDataChunkPos, uint32_t inValueVectorPos, uint32_t outDataChunkPos,
        uint32_t outValueVectorPos, Lists* lists, unique_ptr<PhysicalOperator> prevOperator,
        ExecutionContext& context, uint32_t id, bool isAdjList);
    ~ReadList() override{};
    void printMetricsToJson(nlohmann::json& json, Profiler& profiler) override;

protected:
    void readValuesFromList();

protected:
    uint32_t inDataChunkPos;
    uint32_t inValueVectorPos;
    uint32_t outDataChunkPos;
    uint32_t outValueVectorPos;

    shared_ptr<DataChunk> inDataChunk;
    shared_ptr<ValueVector> inValueVector;
    shared_ptr<DataChunk> outDataChunk;
    shared_ptr<ValueVector> outValueVector;

    Lists* lists;
    unique_ptr<LargeListHandle> largeListHandle;
};

} // namespace processor
} // namespace graphflow
