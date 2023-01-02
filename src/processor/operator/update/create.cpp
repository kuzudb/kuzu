#include "processor/operator/update/create.h"

namespace kuzu {
namespace processor {

void CreateNode::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    for (auto& createNodeInfo : createNodeInfos) {
        createNodeInfo->primaryKeyEvaluator->init(*resultSet, context->memoryManager);
        auto valueVector = resultSet->getValueVector(createNodeInfo->outNodeIDVectorPos);
        outValueVectors.push_back(valueVector.get());
    }
}

bool CreateNode::getNextTuplesInternal() {
    if (!children[0]->getNextTuple()) {
        return false;
    }
    for (auto i = 0u; i < createNodeInfos.size(); ++i) {
        auto createNodeInfo = createNodeInfos[i].get();
        auto nodeTable = createNodeInfo->table;
        createNodeInfo->primaryKeyEvaluator->evaluate();
        auto primaryKeyVector = createNodeInfo->primaryKeyEvaluator->resultVector.get();
        auto nodeOffset = nodeTable->addNodeAndResetProperties(primaryKeyVector);
        auto vector = outValueVectors[i];
        nodeID_t nodeID{nodeOffset, nodeTable->getTableID()};
        vector->setValue(vector->state->selVector->selectedPositions[0], nodeID);
        for (auto& relTable : createNodeInfos[i]->relTablesToInit) {
            relTable->initEmptyRelsForNewNode(nodeID);
        }
    }
    return true;
}

void CreateRel::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    for (auto& createRelInfo : createRelInfos) {
        auto createRelVectors = make_unique<CreateRelVectors>();
        createRelVectors->srcNodeIDVector = resultSet->getValueVector(createRelInfo->srcNodePos);
        createRelVectors->dstNodeIDVector = resultSet->getValueVector(createRelInfo->dstNodePos);
        for (auto& evaluator : createRelInfo->evaluators) {
            evaluator->init(*resultSet, context->memoryManager);
            createRelVectors->propertyVectors.push_back(evaluator->resultVector);
        }
        createRelVectorsPerRel.push_back(std::move(createRelVectors));
    }
}

bool CreateRel::getNextTuplesInternal() {
    if (!children[0]->getNextTuple()) {
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
                       relIDVector->state->selVector->selectedPositions[0] == 0);
                relIDVector->setValue(0, relsStatistics.getNextRelID(transaction));
                relIDVector->setNull(0, false);
            } else {
                createRelInfo->evaluators[j]->evaluate();
            }
        }
        createRelInfo->table->insertRel(createRelVectors->srcNodeIDVector,
            createRelVectors->dstNodeIDVector, createRelVectors->propertyVectors);
        relsStatistics.updateNumRelsByValue(createRelInfo->table->getRelTableID(),
            createRelInfo->srcNodeTableID, createRelInfo->dstNodeTableID,
            1 /* increment numRelsPerDirectionBoundTable by 1 */);
    }
    return true;
}

} // namespace processor
} // namespace kuzu
