#include "processor/operator/scan_column/scan_node_properties.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

bool ScanSingleNodeTableProperties::getNextTuplesInternal() {
    if (!children[0]->getNextTuple()) {
        return false;
    }
    for (auto i = 0u; i < propertyColumns.size(); ++i) {
        auto vector = outPropertyVectors[i];
        // TODO(Everyone): move resetOverflowBuffer to column & list read?
        vector->resetOverflowBuffer();
        propertyColumns[i]->read(transaction, inputNodeIDVector, vector);
    }
    return true;
}

bool ScanMultiNodeTableProperties::getNextTuplesInternal() {
    if (!children[0]->getNextTuple()) {
        return false;
    }
    auto state = inputNodeIDVector->state;
    assert(!state->isFlat()); // Property scans should be sequential and thus on unflat vector only.
    auto tableID =
        inputNodeIDVector->getValue<nodeID_t>(state->selVector->selectedPositions[0]).tableID;
    auto& columns = tableIDToPropertyColumns.at(tableID);
    for (auto i = 0u; i < outPropertyVectors.size(); ++i) {
        auto vector = outPropertyVectors[i];
        vector->resetOverflowBuffer();
        if (columns[i] != nullptr) {
            columns[i]->read(transaction, inputNodeIDVector, vector);
        } else {
            vector->setAllNull();
        }
    }
    return true;
}

} // namespace processor
} // namespace kuzu
