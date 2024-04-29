#include "processor/operator/scan/scan_table.h"

namespace kuzu {
namespace processor {

void ScanTable::initLocalStateInternal(ResultSet* resultSet, ExecutionContext*) {
    nodeIDVector = resultSet->getValueVector(nodeIDPos).get();
    KU_ASSERT(!outVectorsPos.empty());
    outState = resultSet->getValueVector(outVectorsPos[0])->state.get();
}

void ScanTable::initVectors(storage::TableReadState& state, const ResultSet& resultSet) {
    state.nodeIDVector = resultSet.getValueVector(nodeIDPos).get();
    for (auto& pos : outVectorsPos) {
        state.outputVectors.push_back(resultSet.getValueVector(pos).get());
    }
}

} // namespace processor
} // namespace kuzu
