#pragma once

#include "src/expression/include/physical/physical_expression.h"
#include "src/processor/include/physical_plan/expression_mapper.h"
#include "src/processor/include/physical_plan/operator/physical_operator.h"

using namespace graphflow::expression;

namespace graphflow {
namespace processor {

class Filter : public PhysicalOperator {

public:
    Filter(unique_ptr<PhysicalExpression> rootExpr, uint64_t dataChunkToSelectPos,
        unique_ptr<PhysicalOperator> prevOperator);

    void getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<Filter>(ExpressionMapper::clone(*rootExpr, *dataChunks),
            dataChunkToSelectPos, prevOperator->clone());
    }

protected:
    unique_ptr<PhysicalExpression> rootExpr;
    uint64_t dataChunkToSelectPos;
    shared_ptr<DataChunk> dataChunkToSelect;
    uint8_t* exprResult;
};

} // namespace processor
} // namespace graphflow
