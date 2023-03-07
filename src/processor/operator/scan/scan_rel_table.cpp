#include "processor/operator/scan/scan_rel_table.h"

namespace kuzu {
namespace processor {

void ScanRelTable::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    inNodeIDVector = resultSet->getValueVector(inNodeIDVectorPos).get();
    for (auto& dataPos : outputVectorsPos) {
        auto vector = resultSet->getValueVector(dataPos);
        outputVectors.push_back(vector.get());
    }
}

} // namespace processor
} // namespace kuzu
