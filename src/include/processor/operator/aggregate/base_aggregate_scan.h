#pragma once

#include "function/aggregate_function.h"
#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

class BaseAggregateScan : public PhysicalOperator {
public:
    BaseAggregateScan(std::vector<DataPos> aggregatesPos, std::unique_ptr<PhysicalOperator> child,
        uint32_t id, const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::AGGREGATE_SCAN, std::move(child), id,
              paramsString},
          aggregatesPos{std::move(aggregatesPos)} {}

    BaseAggregateScan(
        std::vector<DataPos> aggregatesPos, uint32_t id, const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::AGGREGATE_SCAN, id, paramsString},
          aggregatesPos{std::move(aggregatesPos)} {}

    bool isSource() const override { return true; }

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool getNextTuplesInternal(ExecutionContext* context) override = 0;

    std::unique_ptr<PhysicalOperator> clone() override = 0;

protected:
    void writeAggregateResultToVector(
        common::ValueVector& vector, uint64_t pos, function::AggregateState* aggregateState);

protected:
    std::vector<DataPos> aggregatesPos;
    std::vector<std::shared_ptr<common::ValueVector>> aggregateVectors;
};

} // namespace processor
} // namespace kuzu
