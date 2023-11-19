#include "processor/operator/persistent/delete_executor.h"

using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

void NodeDeleteExecutor::init(ResultSet* resultSet, ExecutionContext* /*context*/) {
    nodeIDVector = resultSet->getValueVector(nodeIDPos).get();
    if (deleteType == DeleteNodeType::DETACH_DELETE) {
        detachDeleteState = std::make_unique<RelDetachDeleteState>();
    }
}

void SingleLabelNodeDeleteExecutor::init(ResultSet* resultSet, ExecutionContext* context) {
    NodeDeleteExecutor::init(resultSet, context);
    auto pkDataType = table->getColumn(table->getPKColumnID())->getDataType();
    pkVector = std::make_unique<ValueVector>(*pkDataType, context->memoryManager);
    pkVector->state = nodeIDVector->state;
}

static void deleteFromRelTable(ExecutionContext* context, DeleteNodeType deleteType,
    RelDataDirection direction, RelTable* relTable, ValueVector* nodeIDVector,
    RelDetachDeleteState* detachDeleteState) {
    switch (deleteType) {
    case DeleteNodeType::DETACH_DELETE: {
        relTable->detachDelete(context->clientContext->getActiveTransaction(), direction,
            nodeIDVector, detachDeleteState);
    } break;
    case DeleteNodeType::DELETE: {
        if (relTable->checkIfNodeHasRels(
                context->clientContext->getActiveTransaction(), direction, nodeIDVector)) {
            throw RuntimeException(
                stringFormat("Deleted nodes has connected edges in the {} direction.",
                    RelDataDirectionUtils::relDirectionToString(direction)));
        }
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
}

void SingleLabelNodeDeleteExecutor::delete_(ExecutionContext* context) {
    KU_ASSERT(nodeIDVector->state->selVector->selectedSize == 1 &&
              pkVector->state == nodeIDVector->state);
    auto nodeIDPos = nodeIDVector->state->selVector->selectedPositions[0];
    if (nodeIDVector->isNull(nodeIDPos)) {
        return;
    }
    for (auto& relTable : fwdRelTables) {
        deleteFromRelTable(context, deleteType, RelDataDirection::FWD, relTable, nodeIDVector,
            detachDeleteState.get());
    }
    for (auto& relTable : bwdRelTables) {
        deleteFromRelTable(context, deleteType, RelDataDirection::BWD, relTable, nodeIDVector,
            detachDeleteState.get());
    }
    table->delete_(context->clientContext->getActiveTransaction(), nodeIDVector, pkVector.get());
}

void MultiLabelNodeDeleteExecutor::init(ResultSet* resultSet, ExecutionContext* context) {
    NodeDeleteExecutor::init(resultSet, context);
    for (auto& [tableID, table] : tableIDToTableMap) {
        auto pkDataType = table->getColumn(table->getPKColumnID())->getDataType();
        pkVectors[tableID] = std::make_unique<ValueVector>(*pkDataType, context->memoryManager);
        pkVectors[tableID]->state = nodeIDVector->state;
    }
}

void MultiLabelNodeDeleteExecutor::delete_(ExecutionContext* context) {
    KU_ASSERT(nodeIDVector->state->selVector->selectedSize == 1);
    auto pos = nodeIDVector->state->selVector->selectedPositions[0];
    if (nodeIDVector->isNull(pos)) {
        return;
    }
    auto nodeID = nodeIDVector->getValue<internalID_t>(pos);
    KU_ASSERT(tableIDToTableMap.contains(nodeID.tableID) && pkVectors.contains(nodeID.tableID));
    auto table = tableIDToTableMap.at(nodeID.tableID);
    auto fwdRelTables = tableIDToFwdRelTablesMap.at(nodeID.tableID);
    auto bwdRelTables = tableIDToBwdRelTablesMap.at(nodeID.tableID);
    for (auto& relTable : fwdRelTables) {
        deleteFromRelTable(context, deleteType, RelDataDirection::FWD, relTable, nodeIDVector,
            detachDeleteState.get());
    }
    for (auto& relTable : bwdRelTables) {
        deleteFromRelTable(context, deleteType, RelDataDirection::BWD, relTable, nodeIDVector,
            detachDeleteState.get());
    }
    table->delete_(context->clientContext->getActiveTransaction(), nodeIDVector,
        pkVectors.at(nodeID.tableID).get());
}

void RelDeleteExecutor::init(ResultSet* resultSet, ExecutionContext* /*context*/) {
    srcNodeIDVector = resultSet->getValueVector(srcNodeIDPos).get();
    dstNodeIDVector = resultSet->getValueVector(dstNodeIDPos).get();
    relIDVector = resultSet->getValueVector(relIDPos).get();
}

void SingleLabelRelDeleteExecutor::delete_(ExecutionContext* context) {
    table->delete_(context->clientContext->getActiveTransaction(), srcNodeIDVector, dstNodeIDVector,
        relIDVector);
}

void MultiLabelRelDeleteExecutor::delete_(ExecutionContext* context) {
    KU_ASSERT(relIDVector->state->isFlat());
    auto pos = relIDVector->state->selVector->selectedPositions[0];
    auto relID = relIDVector->getValue<internalID_t>(pos);
    KU_ASSERT(tableIDToTableMap.contains(relID.tableID));
    auto table = tableIDToTableMap.at(relID.tableID);
    table->delete_(context->clientContext->getActiveTransaction(), srcNodeIDVector, dstNodeIDVector,
        relIDVector);
}

} // namespace processor
} // namespace kuzu
