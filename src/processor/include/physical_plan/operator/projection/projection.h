#pragma once

#include <cassert>

#include "src/expression/include/physical/physical_expression.h"
#include "src/processor/include/physical_plan/expression_mapper.h"
#include "src/processor/include/physical_plan/operator/physical_operator.h"

using namespace graphflow::expression;

namespace graphflow {
namespace processor {

class Projection : public PhysicalOperator {

public:
    Projection(unique_ptr<vector<unique_ptr<PhysicalExpression>>> expressions,
        vector<uint64_t> expressionPosToDataChunkPos, const vector<uint64_t>& discardedDataChunkPos,
        unique_ptr<PhysicalOperator> prevOperator);

    void getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override;

private:
    unique_ptr<vector<unique_ptr<PhysicalExpression>>> expressions;
    vector<uint64_t> expressionPosToDataChunkPos;
    vector<uint64_t> discardedDataChunkPos;
    shared_ptr<DataChunks> discardedDataChunks;
    shared_ptr<DataChunks> inDataChunks;
};

} // namespace processor
} // namespace graphflow
