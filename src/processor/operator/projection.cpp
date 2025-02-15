#include "processor/operator/projection.h"

#include "binder/expression/expression_util.h"
#include "processor/execution_context.h"

using namespace kuzu::evaluator;

namespace kuzu {
namespace processor {

std::string ProjectionPrintInfo::toString() const {
    std::string result = "Expressions: ";
    result += binder::ExpressionUtil::toString(expressions);
    return result;
}

void Projection::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    for (auto i = 0u; i < info.evaluators.size(); ++i) {
        auto& expressionEvaluator = *info.evaluators[i];
        expressionEvaluator.init(*resultSet, context->clientContext);
        auto [dataChunkPos, vectorPos] = info.exprsOutputPos[i];
        auto dataChunk = resultSet->dataChunks[dataChunkPos];
        dataChunk->valueVectors[vectorPos] = expressionEvaluator.resultVector;
    }
}

bool Projection::getNextTuplesInternal(ExecutionContext* context) {
    restoreMultiplicity();
    if (!children[0]->getNextTuple(context)) {
        return false;
    }
    saveMultiplicity();
    for (auto& evaluator : info.evaluators) {
        evaluator->evaluate();
    }
    if (!info.discardedChunkPosSet.empty()) {
        resultSet->multiplicity *=
            resultSet->getNumTuplesWithoutMultiplicity(info.discardedChunkPosSet);
    }
    // The if statement is added to avoid the cost of calculating numTuples when metric is disabled.
    if (metrics->numOutputTuple.enabled) [[unlikely]] {
        if (info.outputChunkPosSet.empty()) {
            // In COUNT(*) case we are projecting away everything and only track multiplicity
            metrics->numOutputTuple.increase(resultSet->multiplicity);
        } else {
            metrics->numOutputTuple.increase(resultSet->getNumTuples(info.outputChunkPosSet));
        }
    }
    return true;
}

} // namespace processor
} // namespace kuzu
