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
        unique_ptr<PhysicalOperator> child, ExecutionContext& context, uint32_t id)
        : PhysicalOperator(move(child), context, id),
          expressions(move(expressions)), expressionsOutputPos{move(expressionsOutputPos)},
          discardedDataChunksPos{move(discardedDataChunksPos)}, prevMultiplicity{0} {}

    PhysicalOperatorType getOperatorType() override { return PROJECTION; }

    shared_ptr<ResultSet> initResultSet() override;

    void reInitToRerunSubPlan() override;

    bool getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override;

private:
    inline void saveMultiplicity() { prevMultiplicity = resultSet->multiplicity; }

    inline void restoreMultiplicity() { resultSet->multiplicity = prevMultiplicity; }

private:
    vector<unique_ptr<ExpressionEvaluator>> expressions;
    vector<DataPos> expressionsOutputPos;
    vector<uint32_t> discardedDataChunksPos;

    uint64_t prevMultiplicity;
    unique_ptr<ResultSet> discardedResultSet;
};

} // namespace processor
} // namespace graphflow
