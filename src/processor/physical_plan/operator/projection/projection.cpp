#include "src/processor/include/physical_plan/operator/projection/projection.h"

namespace graphflow {
namespace processor {

Projection::Projection(unique_ptr<vector<unique_ptr<PhysicalExpression>>> expressions,
    vector<uint64_t> expressionPosToDataChunkPos, const vector<uint64_t>& discardedDataChunkPos,
    unique_ptr<PhysicalOperator> prevOperator)
    : PhysicalOperator(move(prevOperator), PROJECTION), expressions{move(expressions)},
      expressionPosToDataChunkPos(expressionPosToDataChunkPos),
      discardedDataChunkPos(discardedDataChunkPos) {
    dataChunks = make_shared<DataChunks>();
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
    inDataChunks = prevOperator->getDataChunks();
    discardedDataChunks = make_shared<DataChunks>();
    for (auto pos : discardedDataChunkPos) {
        discardedDataChunks->append(inDataChunks->getDataChunk(pos));
    }
}

void Projection::getNextTuples() {
    prevOperator->getNextTuples();
    if (inDataChunks->getNumTuples() > 0) {
        dataChunks->multiplicity = 0;
        for (auto& dataChunk : dataChunks->dataChunks) {
            dataChunk->state->size = 0;
        }
        return;
    }
    for (auto& expression : *expressions) {
        expression->evaluate();
    }
    dataChunks->multiplicity = discardedDataChunks->getNumTuples();
}

unique_ptr<PhysicalOperator> Projection::clone() {
    auto prevOperatorClone = prevOperator->clone();
    auto rootExpressionsCloned = make_unique<vector<unique_ptr<PhysicalExpression>>>();
    for (auto& expr : *expressions) {
        (*rootExpressionsCloned)
            .push_back(ExpressionMapper::clone(*expr, *prevOperatorClone->getDataChunks()));
    }
    return make_unique<Projection>(move(rootExpressionsCloned), expressionPosToDataChunkPos,
        discardedDataChunkPos, move(prevOperatorClone));
}

} // namespace processor
} // namespace graphflow
