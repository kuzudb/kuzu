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
        : PhysicalOperator{PhysicalOperatorType::AGGREGATE_SCAN, std::move(child), id,
              paramsString},
          aggregatesPos{std::move(aggregatesPos)}, aggregateDataTypes{
                                                       std::move(aggregateDataTypes)} {}

    BaseAggregateScan(vector<DataPos> aggregatesPos, vector<DataType> aggregateDataTypes,
        uint32_t id, const string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::AGGREGATE_SCAN, id, paramsString},
          aggregatesPos{std::move(aggregatesPos)}, aggregateDataTypes{
                                                       std::move(aggregateDataTypes)} {}

    bool isSource() const override { return true; }

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool getNextTuplesInternal() override = 0;

    unique_ptr<PhysicalOperator> clone() override = 0;

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
