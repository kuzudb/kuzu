#include "processor/operator/scan_column/adj_column_extend.h"

namespace kuzu {
namespace processor {

bool AdjColumnExtend::getNextTuplesInternal() {
    bool hasAtLeastOneNonNullValue;
    do {
        restoreSelVector(inputNodeIDVector->state->selVector.get());
        if (!children[0]->getNextTuple()) {
            return false;
        }
        saveSelVector(inputNodeIDVector->state->selVector.get());
        outputVector->setAllNull();
        nodeIDColumn->read(transaction, inputNodeIDVector, outputVector);
        hasAtLeastOneNonNullValue = NodeIDVector::discardNull(*outputVector);
    } while (!hasAtLeastOneNonNullValue);
    metrics->numOutputTuple.increase(inputNodeIDVector->state->selVector->selectedSize);
    return true;
}

} // namespace processor
} // namespace kuzu
