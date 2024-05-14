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
    pkVector =
        std::make_unique<ValueVector>(pkDataType, context->clientContext->getMemoryManager());
    pkVector->state = nodeIDVector->state;
}

static void deleteFromRelTable(ExecutionContext* context, DeleteNodeType deleteType,
    RelDataDirection direction, RelTable* relTable, ValueVector* nodeIDVector,
    RelDetachDeleteState* detachDeleteState) {
    switch (deleteType) {
    case DeleteNodeType::DETACH_DELETE: {
        relTable->detachDelete(context->clientContext->getTx(), direction, nodeIDVector,
            detachDeleteState);
    } break;
    case DeleteNodeType::DELETE: {
        relTable->checkIfNodeHasRels(context->clientContext->getTx(), direction, nodeIDVector);
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
}

void SingleLabelNodeDeleteExecutor::delete_(ExecutionContext* context) {
    KU_ASSERT(nodeIDVector->state->getSelVector().getSelSize() == 1 &&
              pkVector->state == nodeIDVector->state);
    auto nodeIDPos = nodeIDVector->state->getSelVector()[0];
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
    auto deleteState = std::make_unique<storage::NodeTableDeleteState>(*nodeIDVector, *pkVector);
    table->delete_(context->clientContext->getTx(), *deleteState);
}

void MultiLabelNodeDeleteExecutor::init(ResultSet* resultSet, ExecutionContext* context) {
    NodeDeleteExecutor::init(resultSet, context);
    for (auto& [tableID, table] : tableIDToTableMap) {
        auto pkDataType = table->getColumn(table->getPKColumnID())->getDataType();
        pkVectors[tableID] =
            std::make_unique<ValueVector>(pkDataType, context->clientContext->getMemoryManager());
        pkVectors[tableID]->state = nodeIDVector->state;
    }
}

void MultiLabelNodeDeleteExecutor::delete_(ExecutionContext* context) {
    KU_ASSERT(nodeIDVector->state->getSelVector().getSelSize() == 1);
    auto pos = nodeIDVector->state->getSelVector()[0];
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
        // TODO(Guodong): For detach delete, there can possibly be a case where the same relTable is
        // in both fwd and bwd rel tables set. the rels can be deleted twice. This is a temporary
        // hack.
        if (deleteType == DeleteNodeType::DETACH_DELETE && fwdRelTables.contains(relTable)) {
            continue;
        }
        deleteFromRelTable(context, deleteType, RelDataDirection::BWD, relTable, nodeIDVector,
            detachDeleteState.get());
    }
    auto deleteState = std::make_unique<storage::NodeTableDeleteState>(*nodeIDVector,
        *pkVectors.at(nodeID.tableID));
    table->delete_(context->clientContext->getTx(), *deleteState);
}

void RelDeleteExecutor::init(ResultSet* resultSet, ExecutionContext* /*context*/) {
    srcNodeIDVector = resultSet->getValueVector(srcNodeIDPos).get();
    dstNodeIDVector = resultSet->getValueVector(dstNodeIDPos).get();
    relIDVector = resultSet->getValueVector(relIDPos).get();
}

void SingleLabelRelDeleteExecutor::delete_(ExecutionContext* context) {
    auto deleteState = std::make_unique<storage::RelTableDeleteState>(*srcNodeIDVector,
        *dstNodeIDVector, *relIDVector);
    table->delete_(context->clientContext->getTx(), *deleteState);
}

void MultiLabelRelDeleteExecutor::delete_(ExecutionContext* context) {
    KU_ASSERT(relIDVector->state->isFlat());
    auto pos = relIDVector->state->getSelVector()[0];
    auto relID = relIDVector->getValue<internalID_t>(pos);
    KU_ASSERT(tableIDToTableMap.contains(relID.tableID));
    auto table = tableIDToTableMap.at(relID.tableID);
    auto deleteState = std::make_unique<storage::RelTableDeleteState>(*srcNodeIDVector,
        *dstNodeIDVector, *relIDVector);
    table->delete_(context->clientContext->getTx(), *deleteState);
}

} // namespace processor
} // namespace kuzu
