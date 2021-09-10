#pragma once

#include "src/processor/include/physical_plan/operator/physical_operator.h"
#include "src/processor/include/task_system/morsel.h"

namespace graphflow {
namespace processor {

class ScanNodeID : public PhysicalOperator {

public:
    explicit ScanNodeID(uint32_t totalNumDataChunks, uint32_t outDataChunkSize,
        uint32_t outDataChunkPos, uint32_t outValueVectorPos, shared_ptr<MorselsDesc>& morselDesc,
        ExecutionContext& context, uint32_t id);

    ScanNodeID(uint32_t outDataChunkSize, uint32_t outDataChunkPos, uint32_t outValueVectorPos,
        shared_ptr<MorselsDesc>& morselsDesc, unique_ptr<PhysicalOperator> prevOperator,
        ExecutionContext& context, uint32_t id);

    void reInitialize() override;

    void getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override {
        return prevOperator ? make_unique<ScanNodeID>(outDataChunkSize, outDataChunkPos,
                                  outValueVectorPos, morsel, prevOperator->clone(), context, id) :
                              make_unique<ScanNodeID>(totalNumDataChunks, outDataChunkSize,
                                  outDataChunkPos, outValueVectorPos, morsel, context, id);
    }

private:
    void initResultSet(uint32_t outDataChunkSize);

private:
    uint32_t totalNumDataChunks;
    uint32_t outDataChunkSize;

    uint32_t outDataChunkPos;
    uint32_t outValueVectorPos;

    shared_ptr<DataChunk> outDataChunk;
    shared_ptr<ValueVector> outValueVector;
    shared_ptr<MorselsDesc> morsel;
};

} // namespace processor
} // namespace graphflow
