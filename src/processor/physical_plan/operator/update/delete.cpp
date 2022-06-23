#include "src/processor/include/physical_plan/operator/update/delete.h"

namespace graphflow {
namespace processor {

shared_ptr<ResultSet> DeleteNodeStructuredProperty::init(ExecutionContext* context) {
    resultSet = PhysicalOperator::init(context);
    for (auto& vectorPos : nodeIDVectorPositions) {
        auto dataChunk = resultSet->dataChunks[vectorPos.dataChunkPos];
        nodeIDVectors.push_back(dataChunk->valueVectors[vectorPos.valueVectorPos].get());
    }
    for (auto& vectorPos : primaryKeyVectorPositions) {
        auto dataChunk = resultSet->dataChunks[vectorPos.dataChunkPos];
        primaryKeyVectors.push_back(dataChunk->valueVectors[vectorPos.valueVectorPos].get());
    }
    return resultSet;
}

bool DeleteNodeStructuredProperty::getNextTuples() {
    metrics->executionTime.start();
    if (!children[0]->getNextTuples()) {
        metrics->executionTime.stop();
        return false;
    }
    for (auto i = 0u; i < nodeTables.size(); ++i) {
        auto nodeTable = nodeTables[i];
        nodeTable->deleteNodes(nodeIDVectors[i], primaryKeyVectors[i]);
    }
    metrics->executionTime.stop();
    return true;
}

} // namespace processor
} // namespace graphflow
