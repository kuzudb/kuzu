#pragma once

#include "src/processor/include/physical_plan/operator/physical_operator.h"

using namespace graphflow::storage;

namespace graphflow {
namespace processor {

class ScanAttribute : public PhysicalOperator {

public:
    ScanAttribute(const DataPos& inDataPos, const DataPos& outDataPos,
        unique_ptr<PhysicalOperator> child, ExecutionContext& context, uint32_t id)
        : PhysicalOperator{move(child), context, id}, inDataPos{inDataPos}, outDataPos{outDataPos} {
    }

    PhysicalOperatorType getOperatorType() override = 0;

    shared_ptr<ResultSet> initResultSet() override;

    void reInitToRerunSubPlan() override;

    void printMetricsToJson(nlohmann::json& json, Profiler& profiler) override;

protected:
    DataPos inDataPos;
    DataPos outDataPos;

    shared_ptr<DataChunk> inDataChunk;
    shared_ptr<ValueVector> inValueVector;
    shared_ptr<ValueVector> outValueVector;
};

} // namespace processor
} // namespace graphflow
