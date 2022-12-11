#include "processor/operator/scan_column/adj_column_extend.h"

namespace kuzu {
namespace processor {

bool ColumnExtendAndScanRelProperties::getNextTuplesInternal() {
    bool hasAtLeastOneNonNullValue;
    // join with adjColumn
    do {
        restoreSelVector(inNodeIDVector->state->selVector.get());
        if (!children[0]->getNextTuple()) {
            return false;
        }
        saveSelVector(inNodeIDVector->state->selVector.get());
        outNodeIDVector->setAllNull();
        adjColumn->read(transaction, inNodeIDVector, outNodeIDVector);
        hasAtLeastOneNonNullValue = NodeIDVector::discardNull(*outNodeIDVector);
    } while (!hasAtLeastOneNonNullValue);
    // scan column properties
    for (auto i = 0u; i < propertyColumns.size(); ++i) {
        auto vector = outPropertyVectors[i];
        vector->resetOverflowBuffer();
        propertyColumns[i]->read(transaction, inNodeIDVector, vector);
    }
    metrics->numOutputTuple.increase(inNodeIDVector->state->selVector->selectedSize);
    return true;
}

} // namespace processor
} // namespace kuzu
