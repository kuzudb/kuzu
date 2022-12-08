#include "processor/operator/scan_column/scan_column.h"

namespace kuzu {
namespace processor {

void BaseScanColumn::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    inputNodeIDVector = resultSet->getValueVector(inputNodeIDVectorPos);
}

void ScanSingleColumn::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    BaseScanColumn::initLocalStateInternal(resultSet, context);
    outputVector = resultSet->getValueVector(outputVectorPos);
}

void ScanMultipleColumns::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    BaseScanColumn::initLocalStateInternal(resultSet, context);
    for (auto& dataPos : outVectorsPos) {
        auto vector = resultSet->getValueVector(dataPos);
        outVectors.push_back(vector);
    }
}

} // namespace processor
} // namespace kuzu
