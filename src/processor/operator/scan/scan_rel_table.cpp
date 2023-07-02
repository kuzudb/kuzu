#include "processor/operator/scan/scan_rel_table.h"

namespace kuzu {
namespace processor {

void ScanRelTable::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    inNodeVector = resultSet->getValueVector(posInfo->inNodeVectorPos).get();
    outNodeVector = resultSet->getValueVector(posInfo->outNodeVectorPos).get();
    for (auto& dataPos : posInfo->outVectorsPos) {
        outVectors.push_back(resultSet->getValueVector(dataPos).get());
    }
}

} // namespace processor
} // namespace kuzu
