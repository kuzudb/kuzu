#include "processor/operator/persistent/delete_executor.h"

using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

void NodeDeleteExecutor::init(ResultSet* resultSet, ExecutionContext* /*context*/) {
    nodeIDVector = resultSet->getValueVector(info.nodeIDPos).get();
    if (info.deleteType == DeleteNodeType::DETACH_DELETE) {
        detachDeleteState = std::make_unique<RelDetachDeleteState>();
    }
}

void SingleLabelNodeDeleteExecutor::init(ResultSet* resultSet, ExecutionContext* context) {
    NodeDeleteExecutor::init(resultSet, context);
    extraInfo.pkVector = resultSet->getValueVector(extraInfo.pkPos).get();
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
              extraInfo.pkVector->state == nodeIDVector->state);
    auto nodeIDPos = nodeIDVector->state->getSelVector()[0];
    if (nodeIDVector->isNull(nodeIDPos)) {
        return;
    }
    auto deleteState =
        std::make_unique<storage::NodeTableDeleteState>(*nodeIDVector, *extraInfo.pkVector);
    if (!extraInfo.table->delete_(context->clientContext->getTx(), *deleteState)) {
        return;
    }
    for (auto& relTable : extraInfo.fwdRelTables) {
        deleteFromRelTable(context, info.deleteType, RelDataDirection::FWD, relTable, nodeIDVector,
            detachDeleteState.get());
    }
    for (auto& relTable : extraInfo.bwdRelTables) {
        deleteFromRelTable(context, info.deleteType, RelDataDirection::BWD, relTable, nodeIDVector,
            detachDeleteState.get());
    }
}

void MultiLabelNodeDeleteExecutor::init(ResultSet* resultSet, ExecutionContext* context) {
    NodeDeleteExecutor::init(resultSet, context);
    for (auto& [tableID, extraInfo] : extraInfos) {
        extraInfo.pkVector = resultSet->getValueVector(extraInfo.pkPos).get();
    }
}

void MultiLabelNodeDeleteExecutor::delete_(ExecutionContext* context) {
    KU_ASSERT(nodeIDVector->state->getSelVector().getSelSize() == 1);
    auto pos = nodeIDVector->state->getSelVector()[0];
    if (nodeIDVector->isNull(pos)) {
        return;
    }
    auto nodeID = nodeIDVector->getValue<internalID_t>(pos);
    auto& extraInfo = extraInfos.at(nodeID.tableID);
    auto deleteState =
        std::make_unique<storage::NodeTableDeleteState>(*nodeIDVector, *extraInfo.pkVector);
    if (!extraInfo.table->delete_(context->clientContext->getTx(), *deleteState)) {
        return;
    }
    KU_ASSERT(extraInfos.contains(nodeID.tableID));
    for (auto& relTable : extraInfo.fwdRelTables) {
        deleteFromRelTable(context, info.deleteType, RelDataDirection::FWD, relTable, nodeIDVector,
            detachDeleteState.get());
    }
    for (auto& relTable : extraInfo.bwdRelTables) {
        // TODO(Guodong): For detach delete, there can possibly be a case where the same relTable is
        // in both fwd and bwd rel tables set. the rels can be deleted twice. This is a temporary
        // hack.
        if (info.deleteType == DeleteNodeType::DETACH_DELETE &&
            extraInfo.fwdRelTables.contains(relTable)) {
            continue;
        }
        deleteFromRelTable(context, info.deleteType, RelDataDirection::BWD, relTable, nodeIDVector,
            detachDeleteState.get());
    }
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
