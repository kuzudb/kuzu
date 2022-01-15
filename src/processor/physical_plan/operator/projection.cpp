#include "src/processor/include/physical_plan/operator/projection.h"

namespace graphflow {
namespace processor {

shared_ptr<ResultSet> Projection::initResultSet() {
    resultSet = children[0]->initResultSet();
    for (auto i = 0u; i < expressions.size(); ++i) {
        auto& expression = *expressions[i];
        expression.initResultSet(*resultSet, *context.memoryManager);
        auto [outDataChunkPos, outValueVectorPos] = expressionsOutputPos[i];
        auto dataChunk = resultSet->dataChunks[outDataChunkPos];
        dataChunk->state = expression.result->state;
        dataChunk->insert(outValueVectorPos, expression.result);
    }
    return resultSet;
}

void Projection::reInitToRerunSubPlan() {
    children[0]->reInitToRerunSubPlan();
}

bool Projection::getNextTuples() {
    metrics->executionTime.start();
    restoreMultiplicity();
    if (!children[0]->getNextTuples()) {
        metrics->executionTime.stop();
        return false;
    }
    saveMultiplicity();
    for (auto& expression : expressions) {
        expression->evaluate();
    }
    if (!discardedDataChunksPos.empty()) {
        resultSet->multiplicity *=
            resultSet->getNumTuplesWithoutMultiplicity(discardedDataChunksPos);
    }
    metrics->executionTime.stop();
    return true;
}

unique_ptr<PhysicalOperator> Projection::clone() {
    vector<unique_ptr<ExpressionEvaluator>> rootExpressionsCloned;
    for (auto& expression : expressions) {
        rootExpressionsCloned.push_back(expression->clone());
    }
    return make_unique<Projection>(move(rootExpressionsCloned), expressionsOutputPos,
        discardedDataChunksPos, children[0]->clone(), context, id);
}

} // namespace processor
} // namespace graphflow
