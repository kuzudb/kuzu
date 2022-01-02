#pragma once

#include "src/expression_evaluator/include/aggregate_expression_evaluator.h"
#include "src/expression_evaluator/include/expression_evaluator.h"
#include "src/function/include/aggregation/aggregation_function.h"
#include "src/processor/include/physical_plan/operator/physical_operator.h"
#include "src/processor/include/physical_plan/operator/sink.h"

using namespace graphflow::evaluator;
using namespace graphflow::function;

namespace graphflow {
namespace processor {

struct AggregationSharedState {
    explicit AggregationSharedState(
        vector<unique_ptr<AggregationState>> aggregationStates, const vector<DataType>& dataTypes)
        : aggregationStates{move(aggregationStates)}, dataTypes{dataTypes}, numGroups{1},
          currentGroupOffset{0} {}

    mutex aggregationSharedStateLock;
    vector<unique_ptr<AggregationState>> aggregationStates;
    vector<DataType> dataTypes;
    uint64_t numGroups;
    uint64_t currentGroupOffset;
};

class SimpleAggregate : public Sink {

public:
    SimpleAggregate(unique_ptr<PhysicalOperator> child, ExecutionContext& context, uint32_t id,
        shared_ptr<AggregationSharedState> aggregationSharedState,
        vector<unique_ptr<AggregateExpressionEvaluator>> aggregationEvaluators);

    PhysicalOperatorType getOperatorType() override { return AGGREGATION; }

    shared_ptr<ResultSet> initResultSet() override;

    void execute() override;

    unique_ptr<PhysicalOperator> clone() override;

    void finalize() override;

private:
    shared_ptr<AggregationSharedState> sharedState;
    vector<unique_ptr<AggregateExpressionEvaluator>> aggregationEvaluators;
    vector<unique_ptr<AggregationState>> aggregationStates;
};

} // namespace processor
} // namespace graphflow
