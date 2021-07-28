#pragma once

#include "src/expression_evaluator/include/expression_evaluator.h"
#include "src/processor/include/physical_plan/operator/filtering_operator.h"
#include "src/processor/include/physical_plan/operator/physical_operator.h"

using namespace graphflow::evaluator;

namespace graphflow {
namespace processor {

class Filter : public PhysicalOperator, public FilteringOperator {

public:
    Filter(unique_ptr<ExpressionEvaluator> rootExpr, uint64_t dataChunkToSelectPos,
        unique_ptr<PhysicalOperator> prevOperator, ExecutionContext& context, uint32_t id);

    void getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override;

protected:
    unique_ptr<ExpressionEvaluator> rootExpr;
    uint8_t* exprResultValues;

private:
    uint64_t dataChunkToSelectPos;
};

} // namespace processor
} // namespace graphflow
