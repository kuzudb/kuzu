#include "processor/operator/base_shortest_path.h"

#include "processor/result/result_set.h"

namespace kuzu {
namespace processor {

shared_ptr<ResultSet> kuzu::processor::BaseShortestPath::init(
    kuzu::processor::ExecutionContext* context) {
    resultSet = PhysicalOperator::init(context);
    srcValueVector =
        resultSet->dataChunks[srcDataPos.dataChunkPos]->getValueVector(srcDataPos.valueVectorPos);
    destValueVector =
        resultSet->dataChunks[destDataPos.dataChunkPos]->getValueVector(destDataPos.valueVectorPos);
    memoryManager = context->memoryManager;
    return resultSet;
}

} // namespace processor
} // namespace kuzu
