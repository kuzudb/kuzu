#include "processor/operator/persistent/set_executor.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

void NodeSetExecutor::init(ResultSet* resultSet, ExecutionContext* context) {
    nodeIDVector = resultSet->getValueVector(nodeIDPos).get();
    if (lhsVectorPos.dataChunkPos != INVALID_DATA_CHUNK_POS) {
        lhsVector = resultSet->getValueVector(lhsVectorPos).get();
    }
    evaluator->init(*resultSet, context->memoryManager);
    rhsVector = evaluator->resultVector.get();
}

std::vector<std::unique_ptr<NodeSetExecutor>> NodeSetExecutor::copy(
    const std::vector<std::unique_ptr<NodeSetExecutor>>& executors) {
    std::vector<std::unique_ptr<NodeSetExecutor>> executorsCopy;
    executorsCopy.reserve(executors.size());
    for (auto& executor : executors) {
        executorsCopy.push_back(executor->copy());
    }
    return executorsCopy;
}

static void writeToPropertyVector(ValueVector* internalIDVector, ValueVector* propertyVector,
    uint32_t propertyVectorPos, ValueVector* rhsVector, uint32_t rhsVectorPos) {
    if (internalIDVector->isNull(propertyVectorPos)) { // No update happened.
        return;
    }
    if (rhsVector->isNull(rhsVectorPos)) { // Update to NULL
        propertyVector->setNull(propertyVectorPos, true);
        return;
    }
    propertyVector->setNull(propertyVectorPos, false);
    propertyVector->copyFromVectorData(propertyVectorPos, rhsVector, rhsVectorPos);
}

void SingleLabelNodeSetExecutor::set(ExecutionContext* context) {
    if (setInfo.columnID == common::INVALID_COLUMN_ID) {
        if (lhsVector != nullptr) {
            for (auto i = 0u; i < nodeIDVector->state->selVector->selectedSize; ++i) {
                auto lhsPos = nodeIDVector->state->selVector->selectedPositions[i];
                lhsVector->setNull(lhsPos, true);
            }
        }
        return;
    }
    evaluator->evaluate();
    KU_ASSERT(nodeIDVector->state->selVector->selectedSize == 1);
    auto lhsPos = nodeIDVector->state->selVector->selectedPositions[0];
    auto rhsPos = rhsVector->state->selVector->selectedPositions[0];
    setInfo.table->update(
        context->clientContext->getActiveTransaction(), setInfo.columnID, nodeIDVector, rhsVector);
    if (lhsVector != nullptr) {
        writeToPropertyVector(nodeIDVector, lhsVector, lhsPos, rhsVector, rhsPos);
    }
}

void MultiLabelNodeSetExecutor::set(ExecutionContext* context) {
    evaluator->evaluate();
    KU_ASSERT(nodeIDVector->state->selVector->selectedSize == 1 &&
              rhsVector->state->selVector->selectedSize == 1);
    auto lhsPos = nodeIDVector->state->selVector->selectedPositions[0];
    auto& nodeID = nodeIDVector->getValue<internalID_t>(lhsPos);
    if (!tableIDToSetInfo.contains(nodeID.tableID)) {
        if (lhsVector != nullptr) {
            lhsVector->setNull(lhsPos, true);
        }
        return;
    }
    auto rhsPos = rhsVector->state->selVector->selectedPositions[0];
    auto& setInfo = tableIDToSetInfo.at(nodeID.tableID);
    setInfo.table->update(
        context->clientContext->getActiveTransaction(), setInfo.columnID, nodeIDVector, rhsVector);
    if (lhsVector != nullptr) {
        KU_ASSERT(lhsVector->state->selVector->selectedSize == 1);
        writeToPropertyVector(nodeIDVector, lhsVector, lhsPos, rhsVector, rhsPos);
    }
}

void RelSetExecutor::init(ResultSet* resultSet, ExecutionContext* context) {
    srcNodeIDVector = resultSet->getValueVector(srcNodeIDPos).get();
    dstNodeIDVector = resultSet->getValueVector(dstNodeIDPos).get();
    relIDVector = resultSet->getValueVector(relIDPos).get();
    if (lhsVectorPos.dataChunkPos != INVALID_DATA_CHUNK_POS) {
        lhsVector = resultSet->getValueVector(lhsVectorPos).get();
    }
    evaluator->init(*resultSet, context->memoryManager);
    rhsVector = evaluator->resultVector.get();
}

std::vector<std::unique_ptr<RelSetExecutor>> RelSetExecutor::copy(
    const std::vector<std::unique_ptr<RelSetExecutor>>& executors) {
    std::vector<std::unique_ptr<RelSetExecutor>> executorsCopy;
    executorsCopy.reserve(executors.size());
    for (auto& executor : executors) {
        executorsCopy.push_back(executor->copy());
    }
    return executorsCopy;
}

// Assume both input vectors are flat. Should be removed eventually.
static void writeToPropertyVector(
    ValueVector* relIDVector, ValueVector* propertyVector, ValueVector* rhsVector) {
    KU_ASSERT(propertyVector->state->selVector->selectedSize == 1);
    auto propertyVectorPos = propertyVector->state->selVector->selectedPositions[0];
    KU_ASSERT(rhsVector->state->selVector->selectedSize == 1);
    auto rhsVectorPos = rhsVector->state->selVector->selectedPositions[0];
    writeToPropertyVector(relIDVector, propertyVector, propertyVectorPos, rhsVector, rhsVectorPos);
}

void SingleLabelRelSetExecutor::set(ExecutionContext* context) {
    if (columnID == INVALID_COLUMN_ID) {
        if (lhsVector != nullptr) {
            auto pos = relIDVector->state->selVector->selectedPositions[0];
            lhsVector->setNull(pos, true);
        }
        return;
    }
    evaluator->evaluate();
    table->update(context->clientContext->getActiveTransaction(), columnID, srcNodeIDVector,
        dstNodeIDVector, relIDVector, rhsVector);
    if (lhsVector != nullptr) {
        writeToPropertyVector(relIDVector, lhsVector, rhsVector);
    }
}

void MultiLabelRelSetExecutor::set(ExecutionContext* context) {
    evaluator->evaluate();
    KU_ASSERT(relIDVector->state->isFlat());
    auto pos = relIDVector->state->selVector->selectedPositions[0];
    auto relID = relIDVector->getValue<internalID_t>(pos);
    if (!tableIDToTableAndColumnID.contains(relID.tableID)) {
        if (lhsVector != nullptr) {
            lhsVector->setNull(pos, true);
        }
        return;
    }
    auto [table, propertyID] = tableIDToTableAndColumnID.at(relID.tableID);
    table->update(context->clientContext->getActiveTransaction(), propertyID, srcNodeIDVector,
        dstNodeIDVector, relIDVector, rhsVector);
    if (lhsVector != nullptr) {
        writeToPropertyVector(relIDVector, lhsVector, rhsVector);
    }
}

} // namespace processor
} // namespace kuzu
