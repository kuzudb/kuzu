#pragma once

#include "function/aggregate/aggregate_function.h"
#include "processor/operator/physical_operator.h"

using namespace kuzu::function;

namespace kuzu {
namespace processor {

class BaseAggregateScan : public PhysicalOperator {

public:
    BaseAggregateScan(vector<DataPos> aggregatesPos, vector<DataType> aggregateDataTypes,
        unique_ptr<PhysicalOperator> child, uint32_t id, const string& paramsString)
        : PhysicalOperator{move(child), id, paramsString}, aggregatesPos{move(aggregatesPos)},
          aggregateDataTypes{move(aggregateDataTypes)} {}

    BaseAggregateScan(vector<DataPos> aggregatesPos, vector<DataType> aggregateDataTypes,
        uint32_t id, const string& paramsString)
        : PhysicalOperator{id, paramsString}, aggregatesPos{move(aggregatesPos)},
          aggregateDataTypes{move(aggregateDataTypes)} {}

    PhysicalOperatorType getOperatorType() override { return AGGREGATE_SCAN; }

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool getNextTuplesInternal() override = 0;

    unique_ptr<PhysicalOperator> clone() override = 0;

    inline double getExecutionTime(Profiler& profiler) const override {
        return profiler.sumAllTimeMetricsWithKey(getTimeMetricKey());
    }

protected:
    void writeAggregateResultToVector(
        ValueVector& vector, uint64_t pos, AggregateState* aggregateState);

protected:
    vector<DataPos> aggregatesPos;
    vector<DataType> aggregateDataTypes;
    vector<shared_ptr<ValueVector>> aggregateVectors;
};

} // namespace processor
} // namespace kuzu
