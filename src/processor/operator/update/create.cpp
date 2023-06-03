#include "processor/operator/update/create.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

void CreateNode::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    for (auto& createNodeInfo : createNodeInfos) {
        if (createNodeInfo->primaryKeyEvaluator != nullptr) {
            createNodeInfo->primaryKeyEvaluator->init(*resultSet, context->memoryManager);
        }
        auto valueVector = resultSet->getValueVector(createNodeInfo->outNodeIDVectorPos);
        outValueVectors.push_back(valueVector.get());
    }
}

bool CreateNode::getNextTuplesInternal(ExecutionContext* context) {
    if (!children[0]->getNextTuple(context)) {
        return false;
    }
    offset_t nodeOffset;
    for (auto i = 0u; i < createNodeInfos.size(); ++i) {
        auto createNodeInfo = createNodeInfos[i].get();
        auto nodeTable = createNodeInfo->table;
        if (createNodeInfo->primaryKeyEvaluator != nullptr) {
            createNodeInfo->primaryKeyEvaluator->evaluate();
            auto primaryKeyVector = createNodeInfo->primaryKeyEvaluator->resultVector.get();
            nodeOffset = nodeTable->addNodeAndResetPropertiesWithPK(primaryKeyVector);
        } else {
            nodeOffset = nodeTable->addNodeAndResetProperties();
        }
        auto vector = outValueVectors[i];
        nodeID_t nodeID{nodeOffset, nodeTable->getTableID()};
        vector->setValue(vector->state->selVector->selectedPositions[0], nodeID);
        for (auto& relTable : createNodeInfo->relTablesToInit) {
            relTable->initEmptyRelsForNewNode(nodeID);
        }
    }
    return true;
}

void CreateRel::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    for (auto& createRelInfo : createRelInfos) {
        auto createRelVectors = std::make_unique<CreateRelVectors>();
        createRelVectors->srcNodeIDVector =
            resultSet->getValueVector(createRelInfo->srcNodePos).get();
        createRelVectors->dstNodeIDVector =
            resultSet->getValueVector(createRelInfo->dstNodePos).get();
        for (auto& evaluator : createRelInfo->evaluators) {
            evaluator->init(*resultSet, context->memoryManager);
            createRelVectors->propertyVectors.push_back(evaluator->resultVector.get());
        }
        createRelVectorsPerRel.push_back(std::move(createRelVectors));
    }
}

bool CreateRel::getNextTuplesInternal(ExecutionContext* context) {
    if (!children[0]->getNextTuple(context)) {
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
                assert(relIDVector->dataType.getLogicalTypeID() == LogicalTypeID::INTERNAL_ID &&
                       relIDVector->state->selVector->selectedPositions[0] == 0);
                relIDVector->setValue(0, relsStatistics.getNextRelOffset(
                                             transaction, createRelInfo->table->getRelTableID()));
                relIDVector->setNull(0, false);
            } else {
                createRelInfo->evaluators[j]->evaluate();
            }
        }
        createRelInfo->table->insertRel(createRelVectors->srcNodeIDVector,
            createRelVectors->dstNodeIDVector, createRelVectors->propertyVectors);
        relsStatistics.updateNumRelsByValue(createRelInfo->table->getRelTableID(),
            1 /* increment numRelsPerDirectionBoundTable by 1 */);
    }
    return true;
}

} // namespace processor
} // namespace kuzu
