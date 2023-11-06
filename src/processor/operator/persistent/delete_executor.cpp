#include "processor/operator/persistent/delete_executor.h"

using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

void NodeDeleteExecutor::init(ResultSet* resultSet, ExecutionContext* /*context*/) {
    nodeIDVector = resultSet->getValueVector(nodeIDPos).get();
}

void SingleLabelNodeDeleteExecutor::init(ResultSet* resultSet, ExecutionContext* context) {
    NodeDeleteExecutor::init(resultSet, context);
    auto pkDataType = table->getColumn(table->getPKColumnID())->getDataType();
    pkVector = std::make_unique<ValueVector>(pkDataType, context->memoryManager);
    pkVector->state = nodeIDVector->state;
}

void SingleLabelNodeDeleteExecutor::delete_(ExecutionContext* context) {
    table->delete_(context->clientContext->getActiveTransaction(), nodeIDVector, pkVector.get());
}

void MultiLabelNodeDeleteExecutor::init(ResultSet* resultSet, ExecutionContext* context) {
    NodeDeleteExecutor::init(resultSet, context);
    for (auto& [tableID, table] : tableIDToTableMap) {
        auto pkDataType = table->getColumn(table->getPKColumnID())->getDataType();
        pkVectors[tableID] = std::make_unique<ValueVector>(pkDataType, context->memoryManager);
        pkVectors[tableID]->state = nodeIDVector->state;
    }
}

void MultiLabelNodeDeleteExecutor::delete_(ExecutionContext* context) {
    KU_ASSERT(nodeIDVector->state->selVector->selectedSize == 1);
    auto pos = nodeIDVector->state->selVector->selectedPositions[0];
    auto nodeID = nodeIDVector->getValue<internalID_t>(pos);
    KU_ASSERT(tableIDToTableMap.contains(nodeID.tableID) && pkVectors.contains(nodeID.tableID));
    auto table = tableIDToTableMap.at(nodeID.tableID);
    table->delete_(context->clientContext->getActiveTransaction(), nodeIDVector,
        pkVectors.at(nodeID.tableID).get());
}

void RelDeleteExecutor::init(ResultSet* resultSet, ExecutionContext* /*context*/) {
    srcNodeIDVector = resultSet->getValueVector(srcNodeIDPos).get();
    dstNodeIDVector = resultSet->getValueVector(dstNodeIDPos).get();
    relIDVector = resultSet->getValueVector(relIDPos).get();
}

void SingleLabelRelDeleteExecutor::delete_() {
    // TODO(Guodong): Fix delete.
    //    table->deleteRel(srcNodeIDVector, dstNodeIDVector, relIDVector);
    //    relsStatistic->updateNumRelsByValue(table->getTableID(), -1);
}

void MultiLabelRelDeleteExecutor::delete_() {
    KU_ASSERT(relIDVector->state->isFlat());
    auto pos = relIDVector->state->selVector->selectedPositions[0];
    auto relID = relIDVector->getValue<internalID_t>(pos);
    KU_ASSERT(tableIDToTableMap.contains(relID.tableID));
    auto [table, statistic] = tableIDToTableMap.at(relID.tableID);
    // TODO(Guodong): Fix delete.
    //    table->deleteRel(srcNodeIDVector, dstNodeIDVector, relIDVector);
    //    KU_ASSERT(table->getTableID() == relID.tableID);
    //    statistic->updateNumRelsByValue(table->getTableID(), -1);
}

} // namespace processor
} // namespace kuzu
