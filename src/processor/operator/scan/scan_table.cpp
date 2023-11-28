#include "processor/operator/scan/scan_table.h"

#include "processor/execution_context.h"
#include "processor/result/result_set.h"

namespace kuzu {
namespace processor {

void ScanTable::initLocalStateInternal(kuzu::processor::ResultSet* resultSet,
    kuzu::processor::ExecutionContext* /*executionContext*/) {
    inVector = resultSet->getValueVector(inVectorPos).get();
    outVectors.reserve(outVectorsPos.size());
    for (auto& pos : outVectorsPos) {
        outVectors.push_back(resultSet->getValueVector(pos).get());
    }
}

} // namespace processor
} // namespace kuzu
