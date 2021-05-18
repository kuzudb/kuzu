#include "src/processor/include/physical_plan/operator/projection/projection.h"

namespace graphflow {
namespace processor {

Projection::Projection(unique_ptr<vector<unique_ptr<ExpressionEvaluator>>> expressions,
    vector<uint64_t> expressionPosToDataChunkPos, const vector<uint64_t>& discardedDataChunkPos,
    unique_ptr<PhysicalOperator> prevOperator)
    : PhysicalOperator(move(prevOperator), PROJECTION), expressions{move(expressions)},
      expressionPosToDataChunkPos(expressionPosToDataChunkPos),
      discardedDataChunkPos(discardedDataChunkPos) {
    resultSet = make_shared<ResultSet>();
    unordered_map<uint64_t, shared_ptr<DataChunk>> posToDataChunkMap;
    for (auto exprPos = 0u; exprPos < expressionPosToDataChunkPos.size(); exprPos++) {
        auto dataChunkPos = expressionPosToDataChunkPos[exprPos];
        if (end(posToDataChunkMap) != posToDataChunkMap.find(dataChunkPos)) {
            auto dataChunk = make_shared<DataChunk>();
            auto exprResult = (*expressions)[exprPos]->result;
            dataChunk->state = exprResult->state;
            dataChunk->append(exprResult);
        } else {
            posToDataChunkMap.at(dataChunkPos)->append((*expressions)[exprPos]->result);
        }
    }
    inResultSet = prevOperator->getResultSet();
    discardedResultSet = make_shared<ResultSet>();
    for (auto pos : discardedDataChunkPos) {
        discardedResultSet->append(inResultSet->dataChunks[pos]);
    }
}

void Projection::getNextTuples() {
    prevOperator->getNextTuples();
    if (inResultSet->getNumTuples() > 0) {
        resultSet->multiplicity = 0;
        for (auto& dataChunk : resultSet->dataChunks) {
            dataChunk->state->size = 0;
        }
        return;
    }
    for (auto& expression : *expressions) {
        expression->evaluate();
    }
    resultSet->multiplicity = discardedResultSet->getNumTuples();
}

unique_ptr<PhysicalOperator> Projection::clone() {
    auto prevOperatorClone = prevOperator->clone();
    auto rootExpressionsCloned = make_unique<vector<unique_ptr<ExpressionEvaluator>>>();
    for (auto& expr : *expressions) {
        (*rootExpressionsCloned)
            .push_back(ExpressionMapper::clone(*expr, *prevOperatorClone->getResultSet()));
    }
    return make_unique<Projection>(move(rootExpressionsCloned), expressionPosToDataChunkPos,
        discardedDataChunkPos, move(prevOperatorClone));
}

} // namespace processor
} // namespace graphflow
