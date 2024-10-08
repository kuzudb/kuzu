#include "processor/operator/projection.h"

#include "binder/expression/expression_util.h"

using namespace kuzu::evaluator;

namespace kuzu {
namespace processor {

std::string ProjectionPrintInfo::toString() const {
    std::string result = "Expressions: ";
    result += binder::ExpressionUtil::toString(expressions);
    return result;
}

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
    // The if statement is added to avoid the cost of calculating numTuples when metric is disabled.
    if (metrics->numOutputTuple.enabled) [[unlikely]] {
        metrics->numOutputTuple.increase(resultSet->getNumTuples(expressionsOutputDataChunksPos));
    }
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
