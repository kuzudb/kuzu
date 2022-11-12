#include "include/projection.h"

namespace kuzu {
namespace processor {

shared_ptr<ResultSet> Projection::init(ExecutionContext* context) {
    resultSet = PhysicalOperator::init(context);
    for (auto i = 0u; i < expressionEvaluators.size(); ++i) {
        auto& expressionEvaluator = *expressionEvaluators[i];
        expressionEvaluator.init(*resultSet, context->memoryManager);
        auto [outDataChunkPos, outValueVectorPos] = expressionsOutputPos[i];
        auto dataChunk = resultSet->dataChunks[outDataChunkPos];
        dataChunk->valueVectors[outValueVectorPos] = expressionEvaluator.resultVector;
    }
    return resultSet;
}

bool Projection::getNextTuples() {
    metrics->executionTime.start();
    restoreMultiplicity();
    if (!children[0]->getNextTuples()) {
        metrics->executionTime.stop();
        return false;
    }
    saveMultiplicity();
    for (auto& expressionEvaluator : expressionEvaluators) {
        expressionEvaluator->evaluate();
    }
    if (!discardedDataChunksPos.empty()) {
        resultSet->multiplicity *=
            resultSet->getNumTuplesWithoutMultiplicity(discardedDataChunksPos);
    }
    metrics->numOutputTuple.increase(1);
    metrics->executionTime.stop();
    return true;
}

unique_ptr<PhysicalOperator> Projection::clone() {
    vector<unique_ptr<BaseExpressionEvaluator>> rootExpressionsCloned;
    for (auto& expressionEvaluator : expressionEvaluators) {
        rootExpressionsCloned.push_back(expressionEvaluator->clone());
    }
    return make_unique<Projection>(move(rootExpressionsCloned), expressionsOutputPos,
        discardedDataChunksPos, children[0]->clone(), id, paramsString);
}

} // namespace processor
} // namespace kuzu
