#include "processor/operator/persistent/delete_executor.h"

using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

void NodeDeleteExecutor::init(ResultSet* resultSet, ExecutionContext* context) {
    nodeIDVector = resultSet->getValueVector(nodeIDPos).get();
}

void SingleLabelNodeDeleteExecutor::init(ResultSet* resultSet, ExecutionContext* context) {
    NodeDeleteExecutor::init(resultSet, context);
    deleteState = std::make_unique<NodeTable::DeleteState>();
    auto pkDataType = table->getPropertyColumn(table->getPKPropertyID())->getDataType();
    deleteState->pkVector = std::make_unique<ValueVector>(pkDataType, context->memoryManager);
    deleteState->pkVector->state = nodeIDVector->state;
}

void SingleLabelNodeDeleteExecutor::delete_(ExecutionContext* context) {
    table->delete_(context->transaction, nodeIDVector, deleteState.get());
}

void MultiLabelNodeDeleteExecutor::init(ResultSet* resultSet, ExecutionContext* context) {
    NodeDeleteExecutor::init(resultSet, context);
    for (auto& [tableID, table] : tableIDToTableMap) {
        deleteStates[tableID] = std::make_unique<NodeTable::DeleteState>();
        auto pkDataType = table->getPropertyColumn(table->getPKPropertyID())->getDataType();
        deleteStates[tableID]->pkVector =
            std::make_unique<ValueVector>(pkDataType, context->memoryManager);
        deleteStates[tableID]->pkVector->state = nodeIDVector->state;
    }
}

void MultiLabelNodeDeleteExecutor::delete_(ExecutionContext* context) {
    assert(nodeIDVector->state->selVector->selectedSize == 1);
    auto pos = nodeIDVector->state->selVector->selectedPositions[0];
    auto nodeID = nodeIDVector->getValue<internalID_t>(pos);
    assert(tableIDToTableMap.contains(nodeID.tableID) && deleteStates.contains(nodeID.tableID));
    auto table = tableIDToTableMap.at(nodeID.tableID);
    table->delete_(context->transaction, nodeIDVector, deleteStates.at(nodeID.tableID).get());
}

void RelDeleteExecutor::init(ResultSet* resultSet, ExecutionContext* context) {
    srcNodeIDVector = resultSet->getValueVector(srcNodeIDPos).get();
    dstNodeIDVector = resultSet->getValueVector(dstNodeIDPos).get();
    relIDVector = resultSet->getValueVector(relIDPos).get();
}

void SingleLabelRelDeleteExecutor::delete_() {
    table->deleteRel(srcNodeIDVector, dstNodeIDVector, relIDVector);
    relsStatistic->updateNumRelsByValue(table->getRelTableID(), -1);
}

void MultiLabelRelDeleteExecutor::delete_() {
    assert(relIDVector->state->isFlat());
    auto pos = relIDVector->state->selVector->selectedPositions[0];
    auto relID = relIDVector->getValue<internalID_t>(pos);
    assert(tableIDToTableMap.contains(relID.tableID));
    auto [table, statistic] = tableIDToTableMap.at(relID.tableID);
    table->deleteRel(srcNodeIDVector, dstNodeIDVector, relIDVector);
    assert(table->getRelTableID() == relID.tableID);
    statistic->updateNumRelsByValue(table->getRelTableID(), -1);
}

} // namespace processor
} // namespace kuzu
