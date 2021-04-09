#include "src/processor/include/physical_plan/operator/projection/projection.h"

namespace graphflow {
namespace processor {

Projection::Projection(vector<unique_ptr<PhysicalExpression>> expressions,
    vector<vector<uint64_t>>& expressionsPerDataChunk, unique_ptr<PhysicalOperator> prevOperator)
    : PhysicalOperator(move(prevOperator), PROJECTION), expressions(move(expressions)),
      expressionsPerInDataChunk(expressionsPerDataChunk) {
    inDataChunks = this->prevOperator->getDataChunks();
    assert(expressionsPerDataChunk.size() == inDataChunks->getNumDataChunks());
    dataChunks = make_shared<DataChunks>();
    discardedDataChunks = make_shared<DataChunks>();
    for (uint64_t i = 0; i < expressionsPerDataChunk.size(); i++) {
        if (expressionsPerDataChunk[i].empty()) {
            discardedDataChunks->append(inDataChunks->getDataChunk(i));
        } else {
            auto outDataChunk = make_shared<DataChunk>(inDataChunks->getDataChunkState(i));
            for (auto& expressionPos : expressionsPerDataChunk[i]) {
                this->expressions[expressionPos]->setExpressionInputDataChunk(
                    inDataChunks->getDataChunk(i));
                this->expressions[expressionPos]->setExpressionResultOwnerState(
                    outDataChunk->state);
                outDataChunk->append(this->expressions[expressionPos]->result);
            }
            dataChunks->append(outDataChunk);
        }
    }
}

void Projection::getNextTuples() {
    prevOperator->getNextTuples();
    if (inDataChunks->getNumTuples() == 0) {
        return;
    }
    for (auto& expression : expressions) {
        expression->evaluate();
    }
    dataChunks->multiplicity = discardedDataChunks->getNumTuples();
}
} // namespace processor
} // namespace graphflow
