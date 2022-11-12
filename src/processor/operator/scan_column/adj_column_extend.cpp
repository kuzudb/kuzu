#include "include/adj_column_extend.h"

namespace kuzu {
namespace processor {

shared_ptr<ResultSet> AdjColumnExtend::init(ExecutionContext* context) {
    resultSet = BaseScanColumn::init(context);
    outputVector = make_shared<ValueVector>(NODE_ID, context->memoryManager);
    inputNodeIDDataChunk->insert(outputVectorPos.valueVectorPos, outputVector);
    return resultSet;
}

bool AdjColumnExtend::getNextTuples() {
    metrics->executionTime.start();
    bool hasAtLeastOneNonNullValue;
    do {
        restoreSelVector(inputNodeIDDataChunk->state->selVector.get());
        if (!children[0]->getNextTuples()) {
            metrics->executionTime.stop();
            return false;
        }
        saveSelVector(inputNodeIDDataChunk->state->selVector.get());
        outputVector->setAllNull();
        nodeIDColumn->read(transaction, inputNodeIDVector, outputVector);
        hasAtLeastOneNonNullValue = NodeIDVector::discardNull(*outputVector);
    } while (!hasAtLeastOneNonNullValue);
    metrics->executionTime.stop();
    metrics->numOutputTuple.increase(inputNodeIDDataChunk->state->selVector->selectedSize);
    return true;
}

} // namespace processor
} // namespace kuzu
