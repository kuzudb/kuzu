#include "src/processor/include/physical_plan/operator/projection/projection.h"

namespace graphflow {
namespace processor {

void Projection::initResultSet(const shared_ptr<ResultSet>& resultSet) {
    PhysicalOperator::initResultSet(resultSet);
    for (auto i = 0u; i < expressions.size(); ++i) {
        auto& expression = *expressions[i];
        expression.initResultSet(*this->resultSet, *context.memoryManager);
        auto [outDataChunkPos, outValueVectorPos] = expressionsOutputPos[i];
        auto dataChunk = this->resultSet->dataChunks[outDataChunkPos];
        dataChunk->state = expression.result->state;
        dataChunk->insert(outValueVectorPos, expression.result);
    }
    discardedResultSet = make_unique<ResultSet>(discardedDataChunksPos.size());
    for (auto i = 0u; i < discardedDataChunksPos.size(); ++i) {
        this->resultSet->dataChunksMask[discardedDataChunksPos[i]] = false;
        discardedResultSet->insert(i, this->resultSet->dataChunks[discardedDataChunksPos[i]]);
    }
}

bool Projection::getNextTuples() {
    metrics->executionTime.start();
    restoreMultiplicity();
    if (!prevOperator->getNextTuples()) {
        metrics->executionTime.stop();
        return false;
    }
    saveMultiplicity();
    for (auto& expression : expressions) {
        expression->evaluate();
    }
    resultSet->multiplicity = discardedResultSet->getNumTuples();
    metrics->executionTime.stop();
    return true;
}

unique_ptr<PhysicalOperator> Projection::clone() {
    vector<unique_ptr<ExpressionEvaluator>> rootExpressionsCloned;
    for (auto& expression : expressions) {
        rootExpressionsCloned.push_back(expression->clone());
    }
    return make_unique<Projection>(move(rootExpressionsCloned), expressionsOutputPos,
        discardedDataChunksPos, prevOperator->clone(), context, id);
}

} // namespace processor
} // namespace graphflow
