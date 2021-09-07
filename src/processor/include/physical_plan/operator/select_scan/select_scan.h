#pragma once

#include "src/processor/include/physical_plan/operator/physical_operator.h"

namespace graphflow {
namespace processor {

/**
 * SelectScan is the scan (first) operator for subquery. It is responsible to copy a flat tuple from
 * outer query into inner query and controls when to terminate inner query.
 */
class SelectScan : public PhysicalOperator {

public:
    SelectScan(uint32_t totalNumDataChunks, const ResultSet* inResultSet,
        vector<pair<uint32_t, uint32_t>> inDataChunkAndValueVectorsPos, uint32_t outDataChunkSize,
        uint32_t outDataChunkPos, vector<uint32_t> outValueVectorsPos, ExecutionContext& context,
        uint32_t id);

    inline void setInResultSet(const ResultSet* resultSet) { inResultSet = resultSet; }

    void reInitialize() override;

    void getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<SelectScan>(totalNumDataChunks, inResultSet,
            inDataChunkAndValueVectorsPos, outDataChunkSize, outDataChunkPos, outValueVectorsPos,
            context, id);
    }

private:
    uint32_t totalNumDataChunks;
    const ResultSet* inResultSet;
    vector<pair<uint32_t, uint32_t>> inDataChunkAndValueVectorsPos;
    uint32_t outDataChunkSize;
    uint32_t outDataChunkPos;
    vector<uint32_t> outValueVectorsPos;

    bool isFirstExecution;
    shared_ptr<DataChunk> outDataChunk;
};

} // namespace processor
} // namespace graphflow