#pragma once

#include <cassert>

#include "src/expression_evaluator/include/expression_evaluator.h"
#include "src/processor/include/physical_plan/operator/physical_operator.h"

using namespace graphflow::evaluator;

namespace graphflow {
namespace processor {

class Projection : public PhysicalOperator {

public:
    Projection(vector<unique_ptr<ExpressionEvaluator>> expressions,
        vector<DataPos> expressionsOutputPos, vector<uint32_t> discardedDataChunksPos,
        shared_ptr<ResultSet> inResultSet, unique_ptr<PhysicalOperator> prevOperator,
        ExecutionContext& context, uint32_t id)
        : PhysicalOperator(move(prevOperator), PROJECTION, context, id),
          expressions(move(expressions)), expressionsOutputPos{move(expressionsOutputPos)},
          discardedDataChunksPos{move(discardedDataChunksPos)}, inResultSet{move(inResultSet)} {}

    void initResultSet(const shared_ptr<ResultSet>& resultSet) override;

    void getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override;

private:
    vector<unique_ptr<ExpressionEvaluator>> expressions;
    vector<DataPos> expressionsOutputPos;
    vector<uint32_t> discardedDataChunksPos;
    shared_ptr<ResultSet> inResultSet;

    shared_ptr<ResultSet> discardedResultSet;
};

} // namespace processor
} // namespace graphflow
