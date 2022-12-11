#include "processor/operator/base_extend.h"

namespace kuzu {
namespace processor {

void BaseExtendAndScanRelProperties::initLocalStateInternal(
    ResultSet* resultSet, ExecutionContext* context) {
    inNodeIDVector = resultSet->getValueVector(inNodeIDVectorPos);
    outNodeIDVector = resultSet->getValueVector(outNodeIDVectorPos);
    for (auto& dataPos : outPropertyVectorsPos) {
        auto vector = resultSet->getValueVector(dataPos);
        outPropertyVectors.push_back(vector);
    }
}

} // namespace processor
} // namespace kuzu
