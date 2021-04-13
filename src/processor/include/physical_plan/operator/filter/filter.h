#pragma once

#include "src/expression/include/physical/physical_expression.h"
#include "src/processor/include/physical_plan/expression_mapper.h"
#include "src/processor/include/physical_plan/operator/physical_operator.h"

using namespace graphflow::expression;

namespace graphflow {
namespace processor {

template<bool IS_AFTER_FLATTEN>
class Filter : public PhysicalOperator {

public:
    Filter(unique_ptr<PhysicalExpression> rootExpr, uint64_t dataChunkToSelectPos,
        unique_ptr<PhysicalOperator> prevOperator);

    void getNextTuples() override;

    unique_ptr<PhysicalOperator> clone();

private:
    void restoreDataChunkToSelectState();
    void saveDataChunkToSelectState();

protected:
    unique_ptr<PhysicalExpression> rootExpr;
    uint64_t dataChunkToSelectPos;
    shared_ptr<DataChunk> dataChunkToSelect;
    uint8_t* exprResult;

private:
    // state to save and store before and after filtering if following flatten operators.
    uint64_t prevInNumSelectedValues;
    unique_ptr<uint64_t[]> prevInSelectedValuesPos;
};

} // namespace processor
} // namespace graphflow
