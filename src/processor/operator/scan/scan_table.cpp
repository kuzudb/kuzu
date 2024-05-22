#include "processor/operator/scan/scan_table.h"

namespace kuzu {
namespace processor {

void ScanTable::initLocalStateInternal(ResultSet* resultSet, ExecutionContext*) {
    nodeIDVector = resultSet->getValueVector(nodeIDPos).get();
    if (outVectorsPos.empty()) {
        outState = nodeIDVector->state.get();
    } else {
        outState = resultSet->getValueVector(outVectorsPos[0])->state.get();
    }
}

void ScanTable::initVectors(storage::TableScanState& state, const ResultSet& resultSet) const {
    state.nodeIDVector = resultSet.getValueVector(nodeIDPos).get();
    for (auto& pos : outVectorsPos) {
        state.outputVectors.push_back(resultSet.getValueVector(pos).get());
    }
}

} // namespace processor
} // namespace kuzu
