#pragma once

#include "src/processor/include/physical_plan/operator/physical_operator.h"
#include "src/processor/include/physical_plan/operator/source_operator.h"

namespace graphflow {
namespace processor {

/**
 * ResultScan is the scan (first) operator for subquery. It is responsible to copy a flat tuple from
 * outer query into inner query and controls when to terminate inner query.
 */
class ResultScan : public PhysicalOperator, public SourceOperator {

public:
    ResultScan(unique_ptr<ResultSetDescriptor> resultSetDescriptor, vector<DataPos> inDataPoses,
        uint32_t outDataChunkPos, vector<uint32_t> outValueVectorsPos, uint32_t id,
        string paramsString)
        : PhysicalOperator{id, paramsString}, SourceOperator{move(resultSetDescriptor)},
          inDataPoses{move(inDataPoses)}, outDataChunkPos{outDataChunkPos},
          outValueVectorsPos{move(outValueVectorsPos)}, isFirstExecution{true} {}

    inline void setResultSetToCopyFrom(const ResultSet* resultSet) {
        resultSetToCopyFrom = resultSet;
    }

    PhysicalOperatorType getOperatorType() override { return RESULT_SCAN; }

    shared_ptr<ResultSet> init(ExecutionContext* context) override;

    void reInitToRerunSubPlan() override;

    bool getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<ResultScan>(resultSetDescriptor->copy(), inDataPoses, outDataChunkPos,
            outValueVectorsPos, id, paramsString);
    }

    inline double getExecutionTime(Profiler& profiler) const override {
        return profiler.sumAllTimeMetricsWithKey(getTimeMetricKey());
    }

private:
    const ResultSet* resultSetToCopyFrom;
    vector<DataPos> inDataPoses;
    uint32_t outDataChunkPos;
    vector<uint32_t> outValueVectorsPos;

    bool isFirstExecution;
    shared_ptr<DataChunk> outDataChunk;
};

} // namespace processor
} // namespace graphflow