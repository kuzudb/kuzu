#include "processor/operator/persistent/insert_executor.h"

#include "storage/stats/rels_store_statistics.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace processor {

NodeInsertExecutor::NodeInsertExecutor(const NodeInsertExecutor& other)
    : table{other.table}, fwdRelTables{other.fwdRelTables}, bwdRelTables{other.bwdRelTables},
      nodeIDVectorPos{other.nodeIDVectorPos}, columnVectorsPos{other.columnVectorsPos},
      conflictAction{other.conflictAction} {
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
        evaluator->init(*resultSet, context->clientContext->getMemoryManager());
        columnDataVectors.push_back(evaluator->resultVector.get());
    }
}

static void writeColumnVector(common::ValueVector* columnVector, common::ValueVector* dataVector) {
    KU_ASSERT(columnVector->state->selVector->selectedSize == 1 &&
              dataVector->state->selVector->selectedSize == 1);
    auto lhsPos = columnVector->state->selVector->selectedPositions[0];
    auto rhsPos = dataVector->state->selVector->selectedPositions[0];
    if (dataVector->isNull(rhsPos)) {
        columnVector->setNull(lhsPos, true);
    } else {
        columnVector->setNull(lhsPos, false);
        columnVector->copyFromVectorData(lhsPos, dataVector, rhsPos);
    }
}

void NodeInsertExecutor::insert(Transaction* tx, ExecutionContext* context) {
    for (auto& evaluator : columnDataEvaluators) {
        evaluator->evaluate(context->clientContext);
    }
    KU_ASSERT(nodeIDVector->state->selVector->selectedSize == 1);
    if (conflictAction == ConflictAction::ON_CONFLICT_DO_NOTHING) {
        auto off = table->validateUniquenessConstraint(tx, columnDataVectors);
        if (off != INVALID_OFFSET) {
            // Conflict. Skip insertion.
            auto nodeIDPos = nodeIDVector->state->selVector->selectedPositions[0];
            nodeIDVector->setNull(nodeIDPos, false);
            nodeIDVector->setValue<nodeID_t>(nodeIDPos, {off, table->getTableID()});
            return;
        }
    }
    table->insert(tx, nodeIDVector, columnDataVectors);
    for (auto i = 0u; i < columnVectors.size(); ++i) {
        auto columnVector = columnVectors[i];
        auto dataVector = columnDataVectors[i];
        if (columnVector == nullptr) {
            // No need to project out lhs vector.
            continue;
        }
        KU_ASSERT(columnVector->state->selVector->selectedSize == 1 &&
                  dataVector->state->selVector->selectedSize == 1);
        if (columnVector->dataType.getLogicalTypeID() == LogicalTypeID::SERIAL) {
            // Lhs vector is serial so there is no corresponding rhs vector.
            auto nodeIDPos = nodeIDVector->state->selVector->selectedPositions[0];
            auto lhsPos = columnVector->state->selVector->selectedPositions[0];
            auto nodeID = nodeIDVector->getValue<nodeID_t>(nodeIDPos);
            columnVector->setNull(lhsPos, false);
            columnVector->setValue<int64_t>(lhsPos, nodeID.offset);
        } else {
            writeColumnVector(columnVector, dataVector);
        }
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
        evaluator->init(*resultSet, context->clientContext->getMemoryManager());
        columnDataVectors.push_back(evaluator->resultVector.get());
    }
}

void RelInsertExecutor::insert(transaction::Transaction* tx, ExecutionContext* context) {
    auto srcNodeIDPos = srcNodeIDVector->state->selVector->selectedPositions[0];
    auto dstNodeIDPos = dstNodeIDVector->state->selVector->selectedPositions[0];
    if (srcNodeIDVector->isNull(srcNodeIDPos) || dstNodeIDVector->isNull(dstNodeIDPos)) {
        // No need to insert.
        for (auto& columnVector : columnVectors) {
            if (columnVector == nullptr) {
                // No need to project out lhs vector.
                continue;
            }
            auto lhsPos = columnVector->state->selVector->selectedPositions[0];
            columnVector->setNull(lhsPos, true);
        }
        return;
    }
    auto offset = relsStatistics->getNextRelOffset(tx, table->getTableID());
    columnDataVectors[0]->setValue(0, offset); // internal ID property
    columnDataVectors[0]->setNull(0, false);
    for (auto i = 1u; i < columnDataEvaluators.size(); ++i) {
        columnDataEvaluators[i]->evaluate(context->clientContext);
    }
    table->insert(tx, srcNodeIDVector, dstNodeIDVector, columnDataVectors);
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
