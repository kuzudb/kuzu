#include "processor/operator/persistent/insert_executor.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

NodeInsertExecutor::NodeInsertExecutor(const NodeInsertExecutor& other)
    : table{other.table}, relTablesToInit{other.relTablesToInit},
      nodeIDVectorPos{other.nodeIDVectorPos}, propertyLhsPositions{other.propertyLhsPositions},
      propertyIDToVectorIdx{other.propertyIDToVectorIdx} {
    for (auto& evaluator : other.propertyRhsEvaluators) {
        propertyRhsEvaluators.push_back(evaluator->clone());
    }
}

void NodeInsertExecutor::init(ResultSet* resultSet, ExecutionContext* context) {
    nodeIDVector = resultSet->getValueVector(nodeIDVectorPos).get();
    for (auto& pos : propertyLhsPositions) {
        if (pos.dataChunkPos != INVALID_DATA_CHUNK_POS) {
            propertyLhsVectors.push_back(resultSet->getValueVector(pos).get());
        } else {
            propertyLhsVectors.push_back(nullptr);
        }
    }
    for (auto& evaluator : propertyRhsEvaluators) {
        evaluator->init(*resultSet, context->memoryManager);
        propertyRhsVectors.push_back(evaluator->resultVector.get());
    }
}

// TODO: consider move to storage
static void writeLhsVectors(const std::vector<common::ValueVector*>& lhsVectors,
    const std::vector<common::ValueVector*>& rhsVectors) {
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

void NodeInsertExecutor::insert(transaction::Transaction* transaction) {
    for (auto& evaluator : propertyRhsEvaluators) {
        evaluator->evaluate();
    }
    table->insert(transaction, nodeIDVector, propertyRhsVectors, propertyIDToVectorIdx);
    for (auto& relTable : relTablesToInit) {
        relTable->initEmptyRelsForNewNode(nodeIDVector);
    }
    writeLhsVectors(propertyLhsVectors, propertyRhsVectors);
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
      dstNodePos{other.dstNodePos}, propertyLhsPositions{other.propertyLhsPositions} {
    for (auto& evaluator : other.propertyRhsEvaluators) {
        propertyRhsEvaluators.push_back(evaluator->clone());
    }
}

void RelInsertExecutor::init(ResultSet* resultSet, ExecutionContext* context) {
    srcNodeIDVector = resultSet->getValueVector(srcNodePos).get();
    dstNodeIDVector = resultSet->getValueVector(dstNodePos).get();
    for (auto& pos : propertyLhsPositions) {
        if (pos.dataChunkPos != INVALID_DATA_CHUNK_POS) {
            propertyLhsVectors.push_back(resultSet->getValueVector(pos).get());
        } else {
            propertyLhsVectors.push_back(nullptr);
        }
    }
    for (auto& evaluator : propertyRhsEvaluators) {
        evaluator->init(*resultSet, context->memoryManager);
        propertyRhsVectors.push_back(evaluator->resultVector.get());
    }
}

void RelInsertExecutor::insert(transaction::Transaction* tx) {
    auto offset = relsStatistics.getNextRelOffset(tx, table->getRelTableID());
    propertyRhsVectors[0]->setValue(0, offset); // internal ID property
    propertyRhsVectors[0]->setNull(0, false);
    for (auto i = 1; i < propertyRhsEvaluators.size(); ++i) {
        propertyRhsEvaluators[i]->evaluate();
    }
    table->insertRel(srcNodeIDVector, dstNodeIDVector, propertyRhsVectors);
    relsStatistics.updateNumRelsByValue(table->getRelTableID(), 1);
    writeLhsVectors(propertyLhsVectors, propertyRhsVectors);
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
