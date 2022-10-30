#include "include/create.h"

namespace graphflow {
namespace processor {

shared_ptr<ResultSet> CreateNode::init(ExecutionContext* context) {
    resultSet = PhysicalOperator::init(context);
    for (auto& createNodeInfo : createNodeInfos) {
        auto pos = createNodeInfo->outNodeIDVectorPos;
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
    for (auto i = 0u; i < createNodeInfos.size(); ++i) {
        auto nodeTable = createNodeInfos[i]->table;
        auto nodeOffset =
            nodeTable->getNodeStatisticsAndDeletedIDs()->addNode(nodeTable->getTableID());
        auto vector = outValueVectors[i];
        auto& nodeIDValue = ((nodeID_t*)vector->values)[vector->state->getPositionOfCurrIdx()];
        nodeIDValue.tableID = nodeTable->getTableID();
        nodeIDValue.offset = nodeOffset;
        nodeTable->getUnstrPropertyLists()->initEmptyPropertyLists(nodeOffset);
        relsStore.initEmptyRelsForNewNode(nodeIDValue);
    }
    metrics->executionTime.stop();
    return true;
}

shared_ptr<ResultSet> CreateRel::init(ExecutionContext* context) {
    resultSet = PhysicalOperator::init(context);
    for (auto& createRelInfo : createRelInfos) {
        vector<shared_ptr<ValueVector>> vectorsToInsert;
        auto srcNodePos = createRelInfo->srcNodePos;
        auto srcNodeVector =
            resultSet->dataChunks[srcNodePos.dataChunkPos]->valueVectors[srcNodePos.valueVectorPos];
        vectorsToInsert.push_back(srcNodeVector);
        auto dstNodePos = createRelInfo->dstNodePos;
        auto dstNodeVector =
            resultSet->dataChunks[dstNodePos.dataChunkPos]->valueVectors[dstNodePos.valueVectorPos];
        vectorsToInsert.push_back(dstNodeVector);
        for (auto& evaluator : createRelInfo->evaluators) {
            evaluator->init(*resultSet, context->memoryManager);
            vectorsToInsert.push_back(evaluator->resultVector);
        }
        vectorsToInsertPerRel.push_back(vectorsToInsert);
    }
    return resultSet;
}

bool CreateRel::getNextTuples() {
    metrics->executionTime.start();
    if (!children[0]->getNextTuples()) {
        metrics->executionTime.stop();
        return false;
    }
    for (auto i = 0u; i < createRelInfos.size(); ++i) {
        auto createRelInfo = createRelInfos[i].get();
        for (auto j = 0u; j < createRelInfo->evaluators.size(); ++j) {
            auto evaluator = createRelInfo->evaluators[j].get();
            // Rel ID is our interval property, so we overwrite relID=$expr with system ID.
            if (j == createRelInfo->relIDEvaluatorIdx) {
                auto relIDVector = evaluator->resultVector;
                assert(relIDVector->dataType.typeID == INT64 &&
                       relIDVector->state->getPositionOfCurrIdx() == 0);
                // TODO(Ziyi): check this getNextRelID() not returning correct value.
                ((int64_t*)relIDVector->values)[0] = (int64_t)relsStatistics.getNextRelID();
                relIDVector->setNull(0, false);
            } else {
                createRelInfo->evaluators[j]->evaluate();
            }
        }
        createRelInfo->table->insertRels(vectorsToInsertPerRel[i]);
    }
    metrics->executionTime.stop();
    return true;
}

} // namespace processor
} // namespace graphflow
