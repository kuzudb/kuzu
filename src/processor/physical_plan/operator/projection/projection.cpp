#include "src/processor/include/physical_plan/operator/projection/projection.h"

namespace graphflow {
namespace processor {

Projection::Projection(uint32_t totalNumDataChunks, vector<uint32_t> outDataChunksSize,
    vector<unique_ptr<ExpressionEvaluator>> expressions,
    vector<pair<uint32_t, uint32_t>> expressionsOutputPos, vector<uint32_t> discardedDataChunksPos,
    unique_ptr<PhysicalOperator> prevOperator, ExecutionContext& context, uint32_t id)
    : PhysicalOperator(move(prevOperator), PROJECTION, context, id),
      totalNumDataChunks{totalNumDataChunks}, outDataChunksSize{move(outDataChunksSize)},
      expressions(move(expressions)), expressionsOutputPos{move(expressionsOutputPos)},
      discardedDataChunksPos{move(discardedDataChunksPos)} {
    inResultSet = this->prevOperator->getResultSet();
    resultSet = make_shared<ResultSet>(totalNumDataChunks);
    for (auto i = 0u; i < this->outDataChunksSize.size(); ++i) {
        resultSet->insert(i, make_shared<DataChunk>(this->outDataChunksSize[i]));
    }
    for (auto i = 0u; i < this->expressions.size(); ++i) {
        auto& expression = *this->expressions[i];
        auto [outDataChunkPos, outValueVectorPos] = this->expressionsOutputPos[i];
        auto dataChunk = resultSet->dataChunks[outDataChunkPos];
        dataChunk->state = expression.result->state;
        dataChunk->insert(outValueVectorPos, expression.result);
    }
    discardedResultSet = make_shared<ResultSet>(this->discardedDataChunksPos.size());
    for (auto i = 0u; i < this->discardedDataChunksPos.size(); ++i) {
        discardedResultSet->insert(i, inResultSet->dataChunks[this->discardedDataChunksPos[i]]);
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
    auto prevOperatorClone = prevOperator->clone();
    vector<unique_ptr<ExpressionEvaluator>> rootExpressionsCloned;
    for (auto& expression : expressions) {
        rootExpressionsCloned.push_back(
            expression->clone(*context.memoryManager, *prevOperatorClone->getResultSet()));
    }
    return make_unique<Projection>(totalNumDataChunks, outDataChunksSize,
        move(rootExpressionsCloned), expressionsOutputPos, discardedDataChunksPos,
        move(prevOperatorClone), context, id);
}

} // namespace processor
} // namespace graphflow
