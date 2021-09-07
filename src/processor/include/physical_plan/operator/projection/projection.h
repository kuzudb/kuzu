#pragma once

#include <cassert>

#include "src/expression_evaluator/include/expression_evaluator.h"
#include "src/processor/include/physical_plan/operator/physical_operator.h"

using namespace graphflow::evaluator;

namespace graphflow {
namespace processor {

class Projection : public PhysicalOperator {

public:
    Projection(uint32_t totalNumDataChunks, vector<uint32_t> outDataChunksSize,
        vector<unique_ptr<ExpressionEvaluator>> expressions,
        vector<pair<uint32_t, uint32_t>> expressionsOutputPos,
        vector<uint32_t> discardedDataChunksPos, unique_ptr<PhysicalOperator> prevOperator,
        ExecutionContext& context, uint32_t id);

    void getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override;

private:
    uint32_t totalNumDataChunks;
    vector<uint32_t> outDataChunksSize;

    vector<unique_ptr<ExpressionEvaluator>> expressions;
    vector<pair<uint32_t, uint32_t>> expressionsOutputPos;
    vector<uint32_t> discardedDataChunksPos;

    shared_ptr<ResultSet> discardedResultSet;
    shared_ptr<ResultSet> inResultSet;
};

} // namespace processor
} // namespace graphflow
