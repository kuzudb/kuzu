#include "src/processor/include/physical_plan/operator/projection/projection.h"

namespace graphflow {
namespace processor {

Projection::Projection(vector<unique_ptr<ExpressionEvaluator>> expressions,
    vector<uint64_t> expressionPosToDataChunkPos, const vector<uint64_t>& discardedDataChunkPos,
    unique_ptr<PhysicalOperator> prevOperator, ExecutionContext& context, uint32_t id)
    : PhysicalOperator(move(prevOperator), PROJECTION, context, id), expressions{move(expressions)},
      expressionPosToDataChunkPos(expressionPosToDataChunkPos),
      discardedDataChunkPos(discardedDataChunkPos) {
    inResultSet = this->prevOperator->getResultSet();
    resultSet = make_shared<ResultSet>();
    unordered_map<uint64_t, shared_ptr<DataChunk>> posToDataChunkMap;
    for (auto exprPos = 0u; exprPos < expressionPosToDataChunkPos.size(); exprPos++) {
        auto dataChunkPos = expressionPosToDataChunkPos[exprPos];
        if (posToDataChunkMap.contains(dataChunkPos)) {
            // Append the expression result vector into an existing data chunk.
            posToDataChunkMap.at(dataChunkPos)->append((this->expressions)[exprPos]->result);
        } else {
            // Create a new data chunk and append the expression result vector into it.
            auto dataChunk = make_shared<DataChunk>();
            auto exprResult = (this->expressions)[exprPos]->result;
            dataChunk->state = exprResult->state;
            dataChunk->append(exprResult);
            posToDataChunkMap[dataChunkPos] = dataChunk;
        }
    }
    resultSet->dataChunks.resize(posToDataChunkMap.size());
    for (auto& entry : posToDataChunkMap) {
        resultSet->dataChunks[entry.first] = move(entry.second);
    }
    discardedResultSet = make_shared<ResultSet>();
    for (auto pos : discardedDataChunkPos) {
        discardedResultSet->append(inResultSet->dataChunks[pos]);
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
    return make_unique<Projection>(move(rootExpressionsCloned), expressionPosToDataChunkPos,
        discardedDataChunkPos, move(prevOperatorClone), context, id);
}

} // namespace processor
} // namespace graphflow
