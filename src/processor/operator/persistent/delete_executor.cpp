#include "processor/operator/persistent/delete_executor.h"

#include <memory>

#include "common/assert.h"
#include "common/vector/value_vector.h"
#include "storage/store/rel_table.h"

using namespace kuzu::common;
using namespace kuzu::storage;
using namespace kuzu::transaction;

namespace kuzu {
namespace processor {

void NodeDeleteInfo::init(const ResultSet& resultSet) {
    nodeIDVector = resultSet.getValueVector(nodeIDPos).get();
}

void NodeTableDeleteInfo::init(const ResultSet& resultSet) {
    pkVector = resultSet.getValueVector(pkPos).get();
}

void NodeTableDeleteInfo::deleteFromRelTable(Transaction* transaction,
    ValueVector* nodeIDVector) const {
    for (auto& relTable : fwdRelTables) {
        relTable->checkIfNodeHasRels(transaction, RelDataDirection::FWD, nodeIDVector);
    }
    for (auto& relTable : bwdRelTables) {
        relTable->checkIfNodeHasRels(transaction, RelDataDirection::BWD, nodeIDVector);
    }
}

void NodeTableDeleteInfo::detachDeleteFromRelTable(Transaction* transaction,
    RelTableDeleteState* detachDeleteState) const {
    for (auto& relTable : fwdRelTables) {
        relTable->detachDelete(transaction, RelDataDirection::FWD, detachDeleteState);
    }
    for (auto& relTable : bwdRelTables) {
        relTable->detachDelete(transaction, RelDataDirection::BWD, detachDeleteState);
    }
}

void NodeDeleteExecutor::init(ResultSet* resultSet, ExecutionContext*) {
    info.init(*resultSet);
    if (info.deleteType == DeleteNodeType::DETACH_DELETE) {
        const auto tempSharedState = std::make_shared<DataChunkState>();
        dstNodeIDVector = std::make_unique<ValueVector>(LogicalType::INTERNAL_ID());
        relIDVector = std::make_unique<ValueVector>(LogicalType::INTERNAL_ID());
        dstNodeIDVector->setState(tempSharedState);
        relIDVector->setState(tempSharedState);
        detachDeleteState = std::make_unique<RelTableDeleteState>(*info.nodeIDVector,
            *dstNodeIDVector, *relIDVector);
    }
}

void SingleLabelNodeDeleteExecutor::init(ResultSet* resultSet, ExecutionContext* context) {
    NodeDeleteExecutor::init(resultSet, context);
    tableInfo.init(*resultSet);
}

void SingleLabelNodeDeleteExecutor::delete_(ExecutionContext* context) {
    KU_ASSERT(tableInfo.pkVector->state == info.nodeIDVector->state);
    auto deleteState =
        std::make_unique<NodeTableDeleteState>(*info.nodeIDVector, *tableInfo.pkVector);
    if (!tableInfo.table->delete_(context->clientContext->getTx(), *deleteState)) {
        return;
    }
    auto transaction = context->clientContext->getTx();
    switch (info.deleteType) {
    case DeleteNodeType::DELETE: {
        tableInfo.deleteFromRelTable(transaction, info.nodeIDVector);
    } break;
    case DeleteNodeType::DETACH_DELETE: {
        tableInfo.detachDeleteFromRelTable(transaction, detachDeleteState.get());
    } break;
    default:
        KU_UNREACHABLE;
    }
}

void MultiLabelNodeDeleteExecutor::init(ResultSet* resultSet, ExecutionContext* context) {
    NodeDeleteExecutor::init(resultSet, context);
    for (auto& [_, tableInfo] : tableInfos) {
        tableInfo.init(*resultSet);
    }
}

void MultiLabelNodeDeleteExecutor::delete_(ExecutionContext* context) {
    auto& nodeIDSelVector = info.nodeIDVector->state->getSelVector();
    KU_ASSERT(nodeIDSelVector.getSelSize() == 1);
    auto pos = nodeIDSelVector[0];
    if (info.nodeIDVector->isNull(pos)) {
        return;
    }
    auto nodeID = info.nodeIDVector->getValue<internalID_t>(pos);
    auto& tableInfo = tableInfos.at(nodeID.tableID);
    auto deleteState =
        std::make_unique<NodeTableDeleteState>(*info.nodeIDVector, *tableInfo.pkVector);
    if (!tableInfo.table->delete_(context->clientContext->getTx(), *deleteState)) {
        return;
    }
    auto transaction = context->clientContext->getTx();
    switch (info.deleteType) {
    case DeleteNodeType::DELETE: {
        tableInfo.deleteFromRelTable(transaction, info.nodeIDVector);
    } break;
    case DeleteNodeType::DETACH_DELETE: {
        tableInfo.detachDeleteFromRelTable(transaction, detachDeleteState.get());
    } break;
    default:
        KU_UNREACHABLE;
    }
}

void RelDeleteInfo::init(const ResultSet& resultSet) {
    srcNodeIDVector = resultSet.getValueVector(srcNodeIDPos).get();
    dstNodeIDVector = resultSet.getValueVector(dstNodeIDPos).get();
    relIDVector = resultSet.getValueVector(relIDPos).get();
}

void RelDeleteExecutor::init(ResultSet* resultSet, ExecutionContext*) {
    info.init(*resultSet);
}

void SingleLabelRelDeleteExecutor::delete_(ExecutionContext* context) {
    auto deleteState = std::make_unique<RelTableDeleteState>(*info.srcNodeIDVector,
        *info.dstNodeIDVector, *info.relIDVector);
    table->delete_(context->clientContext->getTx(), *deleteState);
}

void MultiLabelRelDeleteExecutor::delete_(ExecutionContext* context) {
    auto& idSelVector = info.relIDVector->state->getSelVector();
    KU_ASSERT(idSelVector.getSelSize() == 1);
    auto pos = idSelVector[0];
    auto relID = info.relIDVector->getValue<internalID_t>(pos);
    KU_ASSERT(tableIDToTableMap.contains(relID.tableID));
    auto table = tableIDToTableMap.at(relID.tableID);
    auto deleteState = std::make_unique<RelTableDeleteState>(*info.srcNodeIDVector,
        *info.dstNodeIDVector, *info.relIDVector);
    table->delete_(context->clientContext->getTx(), *deleteState);
}

} // namespace processor
} // namespace kuzu
