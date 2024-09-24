#include "processor/operator/scan/scan_table.h"

namespace kuzu {
namespace processor {

void ScanTable::initVectors(storage::TableScanState& state, const ResultSet& resultSet) const {
    state.nodeIDVector = resultSet.getValueVector(info.nodeIDPos).get();
    for (auto& pos : info.outVectorsPos) {
        state.outputVectors.push_back(resultSet.getValueVector(pos).get());
    }
}

} // namespace processor
} // namespace kuzu
