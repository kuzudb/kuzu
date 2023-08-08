#include "processor/operator/persistent/insert_executor.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

NodeInsertExecutor::NodeInsertExecutor(const NodeInsertExecutor& other)
    : table{other.table}, relTablesToInit{other.relTablesToInit}, outNodeIDVectorPos{
                                                                      other.outNodeIDVectorPos} {
    if (other.primaryKeyEvaluator != nullptr) {
        primaryKeyEvaluator = other.primaryKeyEvaluator->clone();
    }
}

void NodeInsertExecutor::init(ResultSet* resultSet, ExecutionContext* context) {
    if (primaryKeyEvaluator != nullptr) {
        primaryKeyEvaluator->init(*resultSet, context->memoryManager);
        primaryKeyVector = primaryKeyEvaluator->resultVector.get();
    }
    outNodeIDVector = resultSet->getValueVector(outNodeIDVectorPos).get();
}

void NodeInsertExecutor::insert(transaction::Transaction* transaction) {
    if (primaryKeyEvaluator != nullptr) {
        primaryKeyEvaluator->evaluate();
    }
    auto offset = table->insert(transaction, primaryKeyVector);
    common::nodeID_t nodeID{offset, table->getTableID()};
    assert(outNodeIDVector->state->selVector->selectedSize == 1);
    auto pos = outNodeIDVector->state->selVector->selectedPositions[0];
    outNodeIDVector->setValue<common::nodeID_t>(pos, nodeID);
    for (auto& relTable : relTablesToInit) {
        relTable->initEmptyRelsForNewNode(nodeID);
    }
}

std::vector<std::unique_ptr<NodeInsertExecutor>> NodeInsertExecutor::copy(
    const std::vector<std::unique_ptr<NodeInsertExecutor>>& executors) {
    std::vector<std::unique_ptr<NodeInsertExecutor>> executorsCopy;
    executorsCopy.reserve(executors.size());
    for (auto& executor : executors) {
        executorsCopy.push_back(executor->copy());
    }
    return executorsCopy;
}

RelInsertExecutor::RelInsertExecutor(const RelInsertExecutor& other)
    : relsStatistics{other.relsStatistics}, table{other.table}, srcNodePos{other.srcNodePos},
      dstNodePos{other.dstNodePos}, lhsVectorPositions{other.lhsVectorPositions} {
    for (auto& evaluator : other.evaluators) {
        evaluators.push_back(evaluator->clone());
    }
}

void RelInsertExecutor::init(ResultSet* resultSet, ExecutionContext* context) {
    srcNodeIDVector = resultSet->getValueVector(srcNodePos).get();
    dstNodeIDVector = resultSet->getValueVector(dstNodePos).get();
    for (auto& pos : lhsVectorPositions) {
        if (pos.dataChunkPos != INVALID_DATA_CHUNK_POS) {
            lhsVectors.push_back(resultSet->getValueVector(pos).get());
        } else {
            lhsVectors.push_back(nullptr);
        }
    }
    for (auto& evaluator : evaluators) {
        evaluator->init(*resultSet, context->memoryManager);
        rhsVectors.push_back(evaluator->resultVector.get());
    }
}

void RelInsertExecutor::insert(transaction::Transaction* tx) {
    auto offset = relsStatistics.getNextRelOffset(tx, table->getRelTableID());
    rhsVectors[0]->setValue(0, offset);
    rhsVectors[0]->setNull(0, false);
    for (auto i = 1; i < evaluators.size(); ++i) {
        evaluators[i]->evaluate();
    }
    table->insertRel(srcNodeIDVector, dstNodeIDVector, rhsVectors);
    relsStatistics.updateNumRelsByValue(table->getRelTableID(), 1);
    for (auto i = 0u; i < lhsVectors.size(); ++i) {
        auto lhsVector = lhsVectors[i];
        auto rhsVector = rhsVectors[i];
        if (lhsVector == nullptr) {
            continue;
        }
        assert(lhsVector->state->selVector->selectedSize == 1 &&
               rhsVector->state->selVector->selectedSize == 1);
        auto lhsPos = lhsVector->state->selVector->selectedPositions[0];
        auto rhsPos = rhsVector->state->selVector->selectedPositions[0];
        if (rhsVector->isNull(rhsPos)) {
            lhsVector->setNull(lhsPos, true);
        } else {
            lhsVector->setNull(lhsPos, false);
            lhsVector->copyFromVectorData(lhsPos, rhsVector, rhsPos);
        }
    }
}

std::vector<std::unique_ptr<RelInsertExecutor>> RelInsertExecutor::copy(
    const std::vector<std::unique_ptr<RelInsertExecutor>>& executors) {
    std::vector<std::unique_ptr<RelInsertExecutor>> executorsCopy;
    executorsCopy.reserve(executors.size());
    for (auto& executor : executors) {
        executorsCopy.push_back(executor->copy());
    }
    return executorsCopy;
}

} // namespace processor
} // namespace kuzu
