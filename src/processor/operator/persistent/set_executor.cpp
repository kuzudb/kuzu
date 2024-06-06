#include "processor/operator/persistent/set_executor.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

void NodeSetExecutor::init(ResultSet* resultSet, ExecutionContext* context) {
    nodeIDVector = resultSet->getValueVector(info.nodeIDPos).get();
    if (info.lhsPos.isValid()) {
        lhsVector = resultSet->getValueVector(info.lhsPos).get();
    }
    evaluator->init(*resultSet, context->clientContext);
    rhsVector = evaluator->resultVector.get();
    if (info.pkPos.isValid()) {
        pkVector = resultSet->getValueVector(info.pkPos).get();
    }
}

std::vector<std::unique_ptr<NodeSetExecutor>> NodeSetExecutor::copy(
    const std::vector<std::unique_ptr<NodeSetExecutor>>& others) {
    std::vector<std::unique_ptr<NodeSetExecutor>> result;
    for (auto& other : others) {
        result.push_back(other->copy());
    }
    return result;
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
    if (extraInfo.columnID == common::INVALID_COLUMN_ID) {
        if (lhsVector != nullptr) {
            for (auto i = 0u; i < nodeIDVector->state->getSelVector().getSelSize(); ++i) {
                auto lhsPos = nodeIDVector->state->getSelVector()[i];
                lhsVector->setNull(lhsPos, true);
            }
        }
        return;
    }
    evaluator->evaluate();
    KU_ASSERT(nodeIDVector->state->getSelVector().getSelSize() == 1);
    auto lhsPos = nodeIDVector->state->getSelVector()[0];
    auto rhsPos = rhsVector->state->getSelVector()[0];
    auto updateState = std::make_unique<storage::NodeTableUpdateState>(extraInfo.columnID,
        *nodeIDVector, *rhsVector);
    updateState->pkVector = pkVector;
    extraInfo.table->update(context->clientContext->getTx(), *updateState);
    if (lhsVector != nullptr) {
        writeToPropertyVector(nodeIDVector, lhsVector, lhsPos, rhsVector, rhsPos);
    }
}

void MultiLabelNodeSetExecutor::set(ExecutionContext* context) {
    evaluator->evaluate();
    KU_ASSERT(nodeIDVector->state->getSelVector().getSelSize() == 1 &&
              rhsVector->state->getSelVector().getSelSize() == 1);
    auto lhsPos = nodeIDVector->state->getSelVector()[0];
    auto& nodeID = nodeIDVector->getValue<internalID_t>(lhsPos);
    if (!extraInfos.contains(nodeID.tableID)) {
        if (lhsVector != nullptr) {
            lhsVector->setNull(lhsPos, true);
        }
        return;
    }
    auto rhsPos = rhsVector->state->getSelVector()[0];
    auto& extraInfo = extraInfos.at(nodeID.tableID);
    auto updateState = std::make_unique<storage::NodeTableUpdateState>(extraInfo.columnID,
        *nodeIDVector, *rhsVector);
    extraInfo.table->update(context->clientContext->getTx(), *updateState);
    if (lhsVector != nullptr) {
        KU_ASSERT(lhsVector->state->getSelVector().getSelSize() == 1);
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
    evaluator->init(*resultSet, context->clientContext);
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
static void writeToPropertyVector(ValueVector* relIDVector, ValueVector* propertyVector,
    ValueVector* rhsVector) {
    KU_ASSERT(propertyVector->state->getSelVector().getSelSize() == 1);
    auto propertyVectorPos = propertyVector->state->getSelVector()[0];
    KU_ASSERT(rhsVector->state->getSelVector().getSelSize() == 1);
    auto rhsVectorPos = rhsVector->state->getSelVector()[0];
    writeToPropertyVector(relIDVector, propertyVector, propertyVectorPos, rhsVector, rhsVectorPos);
}

void SingleLabelRelSetExecutor::set(ExecutionContext* context) {
    if (columnID == INVALID_COLUMN_ID) {
        if (lhsVector != nullptr) {
            auto pos = relIDVector->state->getSelVector()[0];
            lhsVector->setNull(pos, true);
        }
        return;
    }
    evaluator->evaluate();
    auto updateState = std::make_unique<storage::RelTableUpdateState>(columnID, *srcNodeIDVector,
        *dstNodeIDVector, *relIDVector, *rhsVector);
    table->update(context->clientContext->getTx(), *updateState);
    if (lhsVector != nullptr) {
        writeToPropertyVector(relIDVector, lhsVector, rhsVector);
    }
}

void MultiLabelRelSetExecutor::set(ExecutionContext* context) {
    evaluator->evaluate();
    KU_ASSERT(relIDVector->state->isFlat());
    auto pos = relIDVector->state->getSelVector()[0];
    auto relID = relIDVector->getValue<internalID_t>(pos);
    if (!tableIDToTableAndColumnID.contains(relID.tableID)) {
        if (lhsVector != nullptr) {
            lhsVector->setNull(pos, true);
        }
        return;
    }
    auto [table, columnID] = tableIDToTableAndColumnID.at(relID.tableID);
    auto updateState = std::make_unique<storage::RelTableUpdateState>(columnID, *srcNodeIDVector,
        *dstNodeIDVector, *relIDVector, *rhsVector);
    table->update(context->clientContext->getTx(), *updateState);
    if (lhsVector != nullptr) {
        writeToPropertyVector(relIDVector, lhsVector, rhsVector);
    }
}

} // namespace processor
} // namespace kuzu
