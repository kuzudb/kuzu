#include "processor/operator/update/set_executor.h"

namespace kuzu {
namespace processor {

void NodeSetExecutor::init(ResultSet* resultSet, ExecutionContext* context) {
    nodeIDVector = resultSet->getValueVector(nodeIDPos).get();
    evaluator->init(*resultSet, context->memoryManager);
    rhsVector = evaluator->resultVector.get();
}

void SingleLabelNodeSetExecutor::set() {
    evaluator->evaluate();
    for (auto i = 0u; i < nodeIDVector->state->selVector->selectedSize; ++i) {
        auto nodeIDPos = nodeIDVector->state->selVector->selectedPositions[i];
        auto& nodeID = nodeIDVector->getValue<common::internalID_t>(nodeIDPos);
        auto rhsPos = nodeIDPos;
        if (rhsVector->state->selVector->selectedSize == 1) {
            rhsPos = rhsVector->state->selVector->selectedPositions[0];
        }
        column->write(nodeID.offset, rhsVector, rhsPos);
    }
}

void MultiLabelNodeSetExecutor::set() {
    evaluator->evaluate();
    for (auto i = 0u; i < nodeIDVector->state->selVector->selectedSize; ++i) {
        auto nodeIDPos = nodeIDVector->state->selVector->selectedPositions[i];
        auto& nodeID = nodeIDVector->getValue<common::internalID_t>(nodeIDPos);
        if (!tableIDToColumn.contains(nodeID.tableID)) {
            continue;
        }
        auto column = tableIDToColumn.at(nodeID.tableID);
        auto rhsPos = nodeIDPos;
        if (rhsVector->state->selVector->selectedSize == 1) {
            rhsPos = rhsVector->state->selVector->selectedPositions[0];
        }
        column->write(nodeID.offset, rhsVector, rhsPos);
    }
}

void RelSetExecutor::init(ResultSet* resultSet, ExecutionContext* context) {
    srcNodeIDVector = resultSet->getValueVector(srcNodeIDPos).get();
    dstNodeIDVector = resultSet->getValueVector(dstNodeIDPos).get();
    relIDVector = resultSet->getValueVector(relIDPos).get();
    evaluator->init(*resultSet, context->memoryManager);
    rhsVector = evaluator->resultVector.get();
}

void SingleLabelRelSetExecutor::set() {
    evaluator->evaluate();
    table->updateRel(srcNodeIDVector, dstNodeIDVector, relIDVector, rhsVector, propertyID);
}

void MultiLabelRelSetExecutor::set() {
    evaluator->evaluate();
    assert(relIDVector->state->isFlat());
    auto pos = relIDVector->state->selVector->selectedPositions[0];
    auto relID = relIDVector->getValue<common::internalID_t>(pos);
    if (!tableIDToTableAndPropertyID.contains(relID.tableID)) {
        return;
    }
    auto [table, propertyID] = tableIDToTableAndPropertyID.at(relID.tableID);
    table->updateRel(srcNodeIDVector, dstNodeIDVector, relIDVector, rhsVector, propertyID);
}

} // namespace processor
} // namespace kuzu
