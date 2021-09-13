#include "src/processor/include/physical_plan/operator/projection/projection.h"

namespace graphflow {
namespace processor {

void Projection::initResultSet(const shared_ptr<ResultSet>& resultSet) {
    prevOperator->initResultSet(inResultSet);
    this->resultSet = resultSet;
    for (auto i = 0u; i < expressions.size(); ++i) {
        auto& expression = *expressions[i];
        expression.initResultSet(*inResultSet, *context.memoryManager);
        auto [outDataChunkPos, outValueVectorPos] = expressionsOutputPos[i];
        auto dataChunk = this->resultSet->dataChunks[outDataChunkPos];
        dataChunk->state = expression.result->state;
        dataChunk->insert(outValueVectorPos, expression.result);
    }
    discardedResultSet = make_shared<ResultSet>(discardedDataChunksPos.size());
    for (auto i = 0u; i < discardedDataChunksPos.size(); ++i) {
        discardedResultSet->insert(i, inResultSet->dataChunks[discardedDataChunksPos[i]]);
    }
}

void Projection::getNextTuples() {
    metrics->executionTime.start();
    prevOperator->getNextTuples();
    if (inResultSet->getNumTuples() == 0) {
        for (auto& dataChunk : resultSet->dataChunks) {
            dataChunk->state->selectedSize = 0;
        }
        return;
    }
    for (auto& expression : expressions) {
        expression->evaluate();
    }
    discardedResultSet->multiplicity = inResultSet->multiplicity;
    resultSet->multiplicity = discardedResultSet->getNumTuples();
    metrics->executionTime.stop();
}

unique_ptr<PhysicalOperator> Projection::clone() {
    vector<unique_ptr<ExpressionEvaluator>> rootExpressionsCloned;
    for (auto& expression : expressions) {
        rootExpressionsCloned.push_back(expression->clone());
    }
    auto clonedInResultSet = make_shared<ResultSet>(inResultSet->dataChunks.size());
    for (auto i = 0u; i < inResultSet->dataChunks.size(); ++i) {
        clonedInResultSet->insert(
            i, make_shared<DataChunk>(inResultSet->dataChunks[i]->valueVectors.size()));
    }
    return make_unique<Projection>(move(rootExpressionsCloned), expressionsOutputPos,
        discardedDataChunksPos, clonedInResultSet, prevOperator->clone(), context, id);
}

} // namespace processor
} // namespace graphflow
