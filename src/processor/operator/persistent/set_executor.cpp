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

static void writeToPropertyVector(ValueVector* propertyVector, uint32_t propertyVectorPos,
    ValueVector* rhsVector, uint32_t rhsVectorPos) {
    if (rhsVector->isNull(rhsVectorPos)) {
        propertyVector->setNull(propertyVectorPos, true);
    } else {
        propertyVector->setNull(propertyVectorPos, false);
        propertyVector->copyFromVectorData(propertyVectorPos, rhsVector, rhsVectorPos);
    }
}

void SingleLabelNodeSetExecutor::set(ExecutionContext* context) {
    evaluator->evaluate();
    setInfo.table->update(context->transaction, setInfo.propertyID, nodeIDVector, rhsVector);
    for (auto i = 0u; i < nodeIDVector->state->selVector->selectedSize; ++i) {
        auto lhsPos = nodeIDVector->state->selVector->selectedPositions[i];
        auto rhsPos = lhsPos;
        if (rhsVector->state->selVector->selectedSize == 1) {
            rhsPos = rhsVector->state->selVector->selectedPositions[0];
        }
        if (lhsVector != nullptr) {
            writeToPropertyVector(lhsVector, lhsPos, rhsVector, rhsPos);
        }
    }
}

void MultiLabelNodeSetExecutor::set(ExecutionContext* context) {
    evaluator->evaluate();
    for (auto i = 0u; i < nodeIDVector->state->selVector->selectedSize; ++i) {
        auto lhsPos = nodeIDVector->state->selVector->selectedPositions[i];
        auto& nodeID = nodeIDVector->getValue<internalID_t>(lhsPos);
        if (!tableIDToSetInfo.contains(nodeID.tableID)) {
            if (lhsVector != nullptr) {
                lhsVector->setNull(lhsPos, true);
            }
            continue;
        }
        auto rhsPos = lhsPos;
        if (rhsVector->state->selVector->selectedSize == 1) {
            rhsPos = rhsVector->state->selVector->selectedPositions[0];
        }
        auto& setInfo = tableIDToSetInfo.at(nodeID.tableID);
        setInfo.table->update(
            context->transaction, setInfo.propertyID, nodeID.offset, rhsVector, rhsPos);
        if (lhsVector != nullptr) {
            writeToPropertyVector(lhsVector, lhsPos, rhsVector, rhsPos);
        }
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
static void writeToPropertyVector(ValueVector* propertyVector, ValueVector* rhsVector) {
    assert(propertyVector->state->selVector->selectedSize == 1);
    auto propertyVectorPos = propertyVector->state->selVector->selectedPositions[0];
    assert(rhsVector->state->selVector->selectedSize == 1);
    auto rhsVectorPos = rhsVector->state->selVector->selectedPositions[0];
    writeToPropertyVector(propertyVector, propertyVectorPos, rhsVector, rhsVectorPos);
}

void SingleLabelRelSetExecutor::set() {
    evaluator->evaluate();
    table->updateRel(srcNodeIDVector, dstNodeIDVector, relIDVector, rhsVector, propertyID);
    if (lhsVector != nullptr) {
        writeToPropertyVector(lhsVector, rhsVector);
    }
}

void MultiLabelRelSetExecutor::set() {
    evaluator->evaluate();
    assert(relIDVector->state->isFlat());
    auto pos = relIDVector->state->selVector->selectedPositions[0];
    auto relID = relIDVector->getValue<internalID_t>(pos);
    if (!tableIDToTableAndPropertyID.contains(relID.tableID)) {
        if (lhsVector != nullptr) {
            lhsVector->setNull(pos, true);
        }
        return;
    }
    auto [table, propertyID] = tableIDToTableAndPropertyID.at(relID.tableID);
    table->updateRel(srcNodeIDVector, dstNodeIDVector, relIDVector, rhsVector, propertyID);
    if (lhsVector != nullptr) {
        writeToPropertyVector(lhsVector, rhsVector);
    }
}

} // namespace processor
} // namespace kuzu
