#include "src/processor/include/physical_plan/operator/update/create.h"

namespace graphflow {
namespace processor {

shared_ptr<ResultSet> CreateNode::init(ExecutionContext* context) {
    resultSet = PhysicalOperator::init(context);
    for (auto& pos : outVectorPositions) {
        auto valueVector = make_shared<ValueVector>(NODE_ID, context->memoryManager);
        outValueVectors.push_back(valueVector.get());
        auto dataChunk = resultSet->dataChunks[pos.dataChunkPos];
        dataChunk->state = DataChunkState::getSingleValueDataChunkState();
        dataChunk->insert(pos.valueVectorPos, valueVector);
    }
    return resultSet;
}

bool CreateNode::getNextTuples() {
    metrics->executionTime.start();
    if (!children[0]->getNextTuples()) {
        metrics->executionTime.stop();
        return false;
    }
    for (auto i = 0u; i < nodeTables.size(); ++i) {
        auto nodeTable = nodeTables[i];
        auto nodeOffset =
            nodeTable->getNodeStatisticsAndDeletedIDs()->addNode(nodeTable->getTableID());
        nodeTable->getUnstrPropertyLists()->setPropertyListEmpty(nodeOffset);
        auto vector = outValueVectors[i];
        auto& nodeIDValue = ((nodeID_t*)vector->values)[vector->state->getPositionOfCurrIdx()];
        nodeIDValue.tableID = nodeTable->getTableID();
        nodeIDValue.offset = nodeOffset;
    }
    metrics->executionTime.stop();
    return true;
}

} // namespace processor
} // namespace graphflow
