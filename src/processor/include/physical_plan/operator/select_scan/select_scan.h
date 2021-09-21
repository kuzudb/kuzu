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
    SelectScan(vector<DataPos> inDataPoses, uint32_t outDataChunkPos,
        vector<uint32_t> outValueVectorsPos, ExecutionContext& context, uint32_t id)
        : PhysicalOperator{SELECT_SCAN, context, id}, inDataPoses{move(inDataPoses)},
          outDataChunkPos{outDataChunkPos}, outValueVectorsPos{move(outValueVectorsPos)},
          isFirstExecution{true} {}

    inline void setInResultSet(const ResultSet* resultSet) { inResultSet = resultSet; }

    void initResultSet(const shared_ptr<ResultSet>& resultSet) override;

    void reInitialize() override;

    bool getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<SelectScan>(
            inDataPoses, outDataChunkPos, outValueVectorsPos, context, id);
    }

private:
    const ResultSet* inResultSet;
    vector<DataPos> inDataPoses;
    uint32_t outDataChunkPos;
    vector<uint32_t> outValueVectorsPos;

    bool isFirstExecution;
    shared_ptr<DataChunk> outDataChunk;
};

} // namespace processor
} // namespace graphflow