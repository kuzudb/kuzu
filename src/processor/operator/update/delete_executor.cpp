#include "processor/operator/update/delete_executor.h"

namespace kuzu {
namespace processor {

void NodeDeleteExecutor::init(ResultSet* resultSet, ExecutionContext* context) {
    nodeIDVector = resultSet->getValueVector(nodeIDPos).get();
    primaryKeyVector = resultSet->getValueVector(primaryKeyPos).get();
}

void SingleLabelNodeDeleteExecutor::delete_() {
    for (auto i = 0; i < nodeIDVector->state->selVector->selectedSize; ++i) {
        auto pos = nodeIDVector->state->selVector->selectedPositions[i];
        auto nodeID = nodeIDVector->getValue<common::internalID_t>(pos);
        table->deleteNode(nodeID.offset, primaryKeyVector, pos);
    }
}

void MultiLabelNodeDeleteExecutor::delete_() {
    for (auto i = 0; i < nodeIDVector->state->selVector->selectedSize; ++i) {
        auto pos = nodeIDVector->state->selVector->selectedPositions[i];
        auto nodeID = nodeIDVector->getValue<common::internalID_t>(pos);
        assert(tableIDToTableMap.contains(nodeID.tableID));
        auto table = tableIDToTableMap.at(nodeID.tableID);
        table->deleteNode(nodeID.offset, primaryKeyVector, pos);
    }
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
    auto relID = relIDVector->getValue<common::internalID_t>(pos);
    assert(tableIDToTableMap.contains(relID.tableID));
    auto [table, statistic] = tableIDToTableMap.at(relID.tableID);
    table->deleteRel(srcNodeIDVector, dstNodeIDVector, relIDVector);
    assert(table->getRelTableID() == relID.tableID);
    statistic->updateNumRelsByValue(table->getRelTableID(), -1);
}

} // namespace processor
} // namespace kuzu
