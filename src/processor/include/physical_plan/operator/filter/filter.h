#pragma once

#include "src/expression_evaluator/include/expression_evaluator.h"
#include "src/processor/include/physical_plan/operator/physical_operator.h"

using namespace graphflow::evaluator;

namespace graphflow {
namespace processor {

template<bool IS_AFTER_FLATTEN>
class Filter : public PhysicalOperator {

public:
    Filter(unique_ptr<ExpressionEvaluator> rootExpr, uint64_t dataChunkToSelectPos,
        unique_ptr<PhysicalOperator> prevOperator, ExecutionContext& context, uint32_t id);

    void getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override;

private:
    void restoreDataChunkToSelectState();
    void saveDataChunkToSelectState();

protected:
    unique_ptr<ExpressionEvaluator> rootExpr;
    uint64_t dataChunkToSelectPos;
    shared_ptr<DataChunk> dataChunkToSelect;
    uint8_t* exprResultValues;
    bool* exprResultNullMask;

private:
    // state to save and store before and after filtering if following flatten operators.
    uint64_t prevInNumSelectedValues;
    unique_ptr<uint64_t[]> prevInSelectedValuesPos;
};

} // namespace processor
} // namespace graphflow
