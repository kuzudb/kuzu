#pragma once

#include "src/expression_evaluator/include/base_evaluator.h"
#include "src/processor/include/physical_plan/operator/filtering_operator.h"
#include "src/processor/include/physical_plan/operator/physical_operator.h"

using namespace graphflow::evaluator;

namespace graphflow {
namespace processor {

class Filter : public PhysicalOperator, public FilteringOperator {

public:
    Filter(unique_ptr<BaseExpressionEvaluator> expressionEvaluator, uint32_t dataChunkToSelectPos,
        unique_ptr<PhysicalOperator> child, uint32_t id, const string& paramsString)
        : PhysicalOperator{move(child), id, paramsString},
          FilteringOperator{1 /* numStatesToSave */}, expressionEvaluator{move(
                                                          expressionEvaluator)},
          dataChunkToSelectPos(dataChunkToSelectPos) {}

    PhysicalOperatorType getOperatorType() override { return FILTER; }

    shared_ptr<ResultSet> init(ExecutionContext* context) override;

    bool getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<Filter>(expressionEvaluator->clone(), dataChunkToSelectPos,
            children[0]->clone(), id, paramsString);
    }

private:
    unique_ptr<BaseExpressionEvaluator> expressionEvaluator;
    uint32_t dataChunkToSelectPos;

    shared_ptr<DataChunk> dataChunkToSelect;
};

} // namespace processor
} // namespace graphflow
