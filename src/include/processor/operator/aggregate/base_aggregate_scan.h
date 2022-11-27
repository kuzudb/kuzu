#pragma once

#include "function/aggregate/aggregate_function.h"
#include "processor/operator/physical_operator.h"
#include "processor/operator/source_operator.h"

using namespace kuzu::function;

namespace kuzu {
namespace processor {

class BaseAggregateScan : public PhysicalOperator, public SourceOperator {

public:
    BaseAggregateScan(unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        vector<DataPos> aggregatesPos, vector<DataType> aggregateDataTypes,
        unique_ptr<PhysicalOperator> child, uint32_t id, const string& paramsString)
        : PhysicalOperator{move(child), id, paramsString}, SourceOperator{move(
                                                               resultSetDescriptor)},
          aggregatesPos{move(aggregatesPos)}, aggregateDataTypes{move(aggregateDataTypes)} {}

    BaseAggregateScan(unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        vector<DataPos> aggregatesPos, vector<DataType> aggregateDataTypes, uint32_t id,
        const string& paramsString)
        : PhysicalOperator{id, paramsString}, SourceOperator{move(resultSetDescriptor)},
          aggregatesPos{move(aggregatesPos)}, aggregateDataTypes{move(aggregateDataTypes)} {}

    PhysicalOperatorType getOperatorType() override { return AGGREGATE_SCAN; }

    shared_ptr<ResultSet> init(ExecutionContext* context) override;

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
