#include "include/scan_structured_property.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

bool ScanSingleTablePropertyColumn::getNextTuples() {
    metrics->executionTime.start();
    if (!children[0]->getNextTuples()) {
        metrics->executionTime.stop();
        return false;
    }
    for (auto i = 0u; i < columns.size(); ++i) {
        auto vector = outVectors[i];
        vector->resetOverflowBuffer();
        columns[i]->read(transaction, inputNodeIDVector, vector);
    }
    metrics->executionTime.stop();
    return true;
}

bool ScanMultiTablePropertyColumn::getNextTuples() {
    metrics->executionTime.start();
    if (!children[0]->getNextTuples()) {
        metrics->executionTime.stop();
        return false;
    }
    auto state = inputNodeIDVector->state;
    assert(!state->isFlat()); // TODO(Xiyang): double check this.
    auto tableID =
        inputNodeIDVector->getValue<nodeID_t>(state->selVector->selectedPositions[0]).tableID;
    for (auto i = 0u; i < tableIDToColumns.size(); ++i) {
        auto vector = outVectors[i];
        vector->resetOverflowBuffer();
        if (tableIDToColumns[i].contains(tableID)) {
            tableIDToColumns[i].at(tableID)->read(transaction, inputNodeIDVector, vector);
        } else {
            vector->setAllNull();
        }
    }
    metrics->executionTime.stop();
    return true;
}

} // namespace processor
} // namespace kuzu
