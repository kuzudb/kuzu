#include "processor/operator/persistent/insert_executor.h"

#include "storage/stats/rels_store_statistics.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace processor {

NodeInsertExecutor::NodeInsertExecutor(const NodeInsertExecutor& other)
    : table{other.table}, nodeIDVectorPos{other.nodeIDVectorPos},
      columnVectorsPos{other.columnVectorsPos}, conflictAction{other.conflictAction} {
    for (auto& evaluator : other.columnDataEvaluators) {
        columnDataEvaluators.push_back(evaluator->clone());
    }
}

void NodeInsertExecutor::init(ResultSet* resultSet, ExecutionContext* context) {
    nodeIDVector = resultSet->getValueVector(nodeIDVectorPos).get();
    for (auto& pos : columnVectorsPos) {
        if (pos.isValid()) {
            columnVectors.push_back(resultSet->getValueVector(pos).get());
        } else {
            columnVectors.push_back(nullptr);
        }
    }
    for (auto& evaluator : columnDataEvaluators) {
        evaluator->init(*resultSet, context->clientContext);
        columnDataVectors.push_back(evaluator->resultVector.get());
    }
}

static void writeColumnVector(common::ValueVector* columnVector, common::ValueVector* dataVector) {
    KU_ASSERT(columnVector->state->getSelVector().getSelSize() == 1 &&
              dataVector->state->getSelVector().getSelSize() == 1);
    auto lhsPos = columnVector->state->getSelVector()[0];
    auto rhsPos = dataVector->state->getSelVector()[0];
    if (dataVector->isNull(rhsPos)) {
        columnVector->setNull(lhsPos, true);
    } else {
        columnVector->setNull(lhsPos, false);
        columnVector->copyFromVectorData(lhsPos, dataVector, rhsPos);
    }
}

void NodeInsertExecutor::insert(Transaction* tx) {
    for (auto& evaluator : columnDataEvaluators) {
        evaluator->evaluate();
    }
    KU_ASSERT(nodeIDVector->state->getSelVector().getSelSize() == 1);
    if (checkConflict(tx)) {
        return;
    }
    // TODO: Move pkVector pos to info.
    auto nodeInsertState = std::make_unique<storage::NodeTableInsertState>(*nodeIDVector,
        *columnDataVectors[table->getPKColumnID()], columnDataVectors);
    table->insert(tx, *nodeInsertState);
    writeResult();
}

void NodeInsertExecutor::skipInsert() {
    for (auto& evaluator : columnDataEvaluators) {
        evaluator->evaluate();
    }
    nodeIDVector->setNull(nodeIDVector->state->getSelVector()[0], false);
    writeResult();
}

bool NodeInsertExecutor::checkConflict(Transaction* transaction) {
    if (conflictAction == ConflictAction::ON_CONFLICT_DO_NOTHING) {
        auto off = table->validateUniquenessConstraint(transaction, columnDataVectors);
        if (off != INVALID_OFFSET) {
            // Conflict. Skip insertion.
            auto nodeIDPos = nodeIDVector->state->getSelVector()[0];
            nodeIDVector->setNull(nodeIDPos, false);
            nodeIDVector->setValue<nodeID_t>(nodeIDPos, {off, table->getTableID()});
            return true;
        }
    }
    return false;
}

void NodeInsertExecutor::writeResult() {
    for (auto i = 0u; i < columnVectors.size(); ++i) {
        auto columnVector = columnVectors[i];
        auto dataVector = columnDataVectors[i];
        if (columnVector == nullptr) {
            // No need to project out lhs vector.
            continue;
        }
        KU_ASSERT(columnVector->state->getSelVector().getSelSize() == 1 &&
                  dataVector->state->getSelVector().getSelSize() == 1);
        writeColumnVector(columnVector, dataVector);
    }
}

RelInsertExecutor::RelInsertExecutor(const RelInsertExecutor& other)
    : relsStatistics{other.relsStatistics}, table{other.table}, srcNodePos{other.srcNodePos},
      dstNodePos{other.dstNodePos}, columnVectorsPos{other.columnVectorsPos},
      srcNodeIDVector{nullptr}, dstNodeIDVector{nullptr} {
    for (auto& evaluator : other.columnDataEvaluators) {
        columnDataEvaluators.push_back(evaluator->clone());
    }
}

void RelInsertExecutor::init(ResultSet* resultSet, ExecutionContext* context) {
    srcNodeIDVector = resultSet->getValueVector(srcNodePos).get();
    dstNodeIDVector = resultSet->getValueVector(dstNodePos).get();
    for (auto& pos : columnVectorsPos) {
        if (pos.dataChunkPos != INVALID_DATA_CHUNK_POS) {
            columnVectors.push_back(resultSet->getValueVector(pos).get());
        } else {
            columnVectors.push_back(nullptr);
        }
    }
    for (auto& evaluator : columnDataEvaluators) {
        evaluator->init(*resultSet, context->clientContext);
        columnDataVectors.push_back(evaluator->resultVector.get());
    }
}

void RelInsertExecutor::insert(transaction::Transaction* tx) {
    auto srcNodeIDPos = srcNodeIDVector->state->getSelVector()[0];
    auto dstNodeIDPos = dstNodeIDVector->state->getSelVector()[0];
    if (srcNodeIDVector->isNull(srcNodeIDPos) || dstNodeIDVector->isNull(dstNodeIDPos)) {
        // No need to insert.
        for (auto& columnVector : columnVectors) {
            if (columnVector == nullptr) {
                // No need to project out lhs vector.
                continue;
            }
            auto lhsPos = columnVector->state->getSelVector()[0];
            columnVector->setNull(lhsPos, true);
        }
        return;
    }
    auto offset = relsStatistics->getNextRelOffset(tx, table->getTableID());
    columnDataVectors[0]->setValue<internalID_t>(0, internalID_t{offset, table->getTableID()});
    columnDataVectors[0]->setNull(0, false);
    for (auto i = 1u; i < columnDataEvaluators.size(); ++i) {
        columnDataEvaluators[i]->evaluate();
    }
    auto insertState = std::make_unique<storage::RelTableInsertState>(*srcNodeIDVector,
        *dstNodeIDVector, columnDataVectors);
    table->insert(tx, *insertState);
    writeResult();
}

void RelInsertExecutor::skipInsert() {
    for (auto i = 1u; i < columnDataEvaluators.size(); ++i) {
        columnDataEvaluators[i]->evaluate();
    }
    writeResult();
}

void RelInsertExecutor::writeResult() {
    for (auto i = 0u; i < columnVectors.size(); ++i) {
        auto columnVector = columnVectors[i];
        auto dataVector = columnDataVectors[i];
        if (columnVector == nullptr) {
            // No need to project out lhs vector.
            continue;
        }
        writeColumnVector(columnVector, dataVector);
    }
}

} // namespace processor
} // namespace kuzu
