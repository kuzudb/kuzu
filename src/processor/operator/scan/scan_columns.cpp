#include "processor/operator/scan/scan_columns.h"

namespace kuzu {
namespace processor {

void ScanColumns::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    inputNodeIDVector = resultSet->getValueVector(inputNodeIDVectorPos).get();
    for (auto& dataPos : outPropertyVectorsPos) {
        auto vector = resultSet->getValueVector(dataPos);
        outPropertyVectors.push_back(vector.get());
    }
}

} // namespace processor
} // namespace kuzu
