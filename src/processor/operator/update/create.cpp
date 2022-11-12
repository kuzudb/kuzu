#include "include/create.h"

namespace kuzu {
namespace processor {

shared_ptr<ResultSet> CreateNode::init(ExecutionContext* context) {
    resultSet = PhysicalOperator::init(context);
    for (auto& createNodeInfo : createNodeInfos) {
        createNodeInfo->primaryKeyEvaluator->init(*resultSet, context->memoryManager);
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
        auto createNodeInfo = createNodeInfos[i].get();
        auto nodeTable = createNodeInfo->table;
        createNodeInfo->primaryKeyEvaluator->evaluate();
        auto primaryKeyVector = createNodeInfo->primaryKeyEvaluator->resultVector.get();
        auto nodeOffset = nodeTable->addNodeAndResetProperties(primaryKeyVector);
        auto vector = outValueVectors[i];
        auto& nodeIDValue = ((nodeID_t*)vector->values)[vector->state->getPositionOfCurrIdx()];
        nodeIDValue.tableID = nodeTable->getTableID();
        nodeIDValue.offset = nodeOffset;
        for (auto& relTable : createNodeInfos[i]->relTablesToInit) {
            relTable->initEmptyRelsForNewNode(nodeIDValue);
        }
    }
    metrics->executionTime.stop();
    return true;
}

shared_ptr<ResultSet> CreateRel::init(ExecutionContext* context) {
    resultSet = PhysicalOperator::init(context);
    for (auto& createRelInfo : createRelInfos) {
        auto srcNodePos = createRelInfo->srcNodePos;
        auto createRelVectors = make_unique<CreateRelVectors>();
        createRelVectors->srcNodeIDVector =
            resultSet->dataChunks[srcNodePos.dataChunkPos]->valueVectors[srcNodePos.valueVectorPos];
        auto dstNodePos = createRelInfo->dstNodePos;
        createRelVectors->dstNodeIDVector =
            resultSet->dataChunks[dstNodePos.dataChunkPos]->valueVectors[dstNodePos.valueVectorPos];
        for (auto& evaluator : createRelInfo->evaluators) {
            evaluator->init(*resultSet, context->memoryManager);
            createRelVectors->propertyVectors.push_back(evaluator->resultVector);
        }
        createRelVectorsPerRel.push_back(std::move(createRelVectors));
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
        auto createRelVectors = createRelVectorsPerRel[i].get();
        for (auto j = 0u; j < createRelInfo->evaluators.size(); ++j) {
            auto evaluator = createRelInfo->evaluators[j].get();
            // Rel ID is our interval property, so we overwrite relID=$expr with system ID.
            if (j == createRelInfo->relIDEvaluatorIdx) {
                auto relIDVector = evaluator->resultVector;
                assert(relIDVector->dataType.typeID == INT64 &&
                       relIDVector->state->getPositionOfCurrIdx() == 0);
                ((int64_t*)relIDVector->values)[0] = relsStatistics.getNextRelID(transaction);
                relIDVector->setNull(0, false);
                // TODO(Ziyi): the following two functions should be wrapped into 1 (issue #891)
                relsStatistics.incrementNumRelsPerDirectionBoundTableByOne(
                    createRelInfo->table->getRelTableID(), createRelInfo->srcNodeTableID,
                    createRelInfo->dstNodeTableID);
                relsStatistics.incrementNumRelsByOneForTable(createRelInfo->table->getRelTableID());

            } else {
                createRelInfo->evaluators[j]->evaluate();
            }
        }
        createRelInfo->table->insertRels(createRelVectors->srcNodeIDVector,
            createRelVectors->dstNodeIDVector, createRelVectors->propertyVectors);
    }
    metrics->executionTime.stop();
    return true;
}

} // namespace processor
} // namespace kuzu
