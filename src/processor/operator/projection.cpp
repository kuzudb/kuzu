#include "processor/operator/projection.h"

using namespace kuzu::evaluator;

namespace kuzu {
namespace processor {

void Projection::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    for (auto i = 0u; i < expressionEvaluators.size(); ++i) {
        auto& expressionEvaluator = *expressionEvaluators[i];
        expressionEvaluator.init(*resultSet, context->clientContext);
        auto [outDataChunkPos, outValueVectorPos] = expressionsOutputPos[i];
        auto dataChunk = resultSet->dataChunks[outDataChunkPos];
        dataChunk->valueVectors[outValueVectorPos] = expressionEvaluator.resultVector;
    }
}

bool Projection::getNextTuplesInternal(ExecutionContext* context) {
    restoreMultiplicity();
    if (!children[0]->getNextTuple(context)) {
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
    return true;
}

std::unique_ptr<PhysicalOperator> Projection::clone() {
    std::vector<std::unique_ptr<ExpressionEvaluator>> rootExpressionsCloned;
    rootExpressionsCloned.reserve(expressionEvaluators.size());
    for (auto& expressionEvaluator : expressionEvaluators) {
        rootExpressionsCloned.push_back(expressionEvaluator->clone());
    }
    return make_unique<Projection>(std::move(rootExpressionsCloned), expressionsOutputPos,
        discardedDataChunksPos, children[0]->clone(), id, printInfo->copy());
}

} // namespace processor
} // namespace kuzu
