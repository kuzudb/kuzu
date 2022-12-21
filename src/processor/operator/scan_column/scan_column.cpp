#include "processor/operator/scan_column/scan_column.h"

namespace kuzu {
namespace processor {

void BaseScanColumn::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    inputNodeIDVector = resultSet->getValueVector(inputNodeIDVectorPos);
}

void ScanMultipleColumns::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    BaseScanColumn::initLocalStateInternal(resultSet, context);
    for (auto& dataPos : outPropertyVectorsPos) {
        auto vector = resultSet->getValueVector(dataPos);
        outPropertyVectors.push_back(vector);
    }
}

} // namespace processor
} // namespace kuzu
