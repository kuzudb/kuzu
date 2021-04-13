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
    Projection(vector<unique_ptr<PhysicalExpression>> expressions,
        vector<vector<uint64_t>>& expressionsPerDataChunk,
        unique_ptr<PhysicalOperator> prevOperator);

    void getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override {
        vector<unique_ptr<PhysicalExpression>> clonedExpressions;
        for (auto& expr : expressions) {
            clonedExpressions.push_back(ExpressionMapper::clone(*expr, *dataChunks));
        }
        return make_unique<Projection>(
            move(clonedExpressions), expressionsPerInDataChunk, prevOperator->clone());
    }

private:
    vector<unique_ptr<PhysicalExpression>> expressions;
    vector<vector<uint64_t>> expressionsPerInDataChunk;

    shared_ptr<DataChunks> inDataChunks;
    shared_ptr<DataChunks> discardedDataChunks;
};

} // namespace processor
} // namespace graphflow
