#pragma once

#include "src/expression_evaluator/include/expression_evaluator.h"
#include "src/processor/include/physical_plan/operator/filtering_operator.h"
#include "src/processor/include/physical_plan/operator/physical_operator.h"

using namespace graphflow::evaluator;

namespace graphflow {
namespace processor {

class Filter : public PhysicalOperator, public FilteringOperator {

public:
    Filter(unique_ptr<ExpressionEvaluator> rootExpr, uint32_t dataChunkToSelectPos,
        unique_ptr<PhysicalOperator> child, ExecutionContext& context, uint32_t id)
        : PhysicalOperator{move(child), context, id}, FilteringOperator{}, rootExpr{move(rootExpr)},
          dataChunkToSelectPos(dataChunkToSelectPos) {}

    PhysicalOperatorType getOperatorType() override { return FILTER; }

    shared_ptr<ResultSet> initResultSet() override;

    void reInitToRerunSubPlan() override;

    bool getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<Filter>(
            rootExpr->clone(), dataChunkToSelectPos, children[0]->clone(), context, id);
    }

private:
    unique_ptr<ExpressionEvaluator> rootExpr;
    uint32_t dataChunkToSelectPos;

    shared_ptr<DataChunk> dataChunkToSelect;
};

} // namespace processor
} // namespace graphflow
