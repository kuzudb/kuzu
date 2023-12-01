#include "processor/operator/persistent/insert_executor.h"

#include "storage/stats/rels_store_statistics.h"
#include "storage/storage_utils.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

NodeInsertExecutor::NodeInsertExecutor(const NodeInsertExecutor& other)
    : table{other.table}, fwdRelTablesToInit{other.fwdRelTablesToInit},
      bwdRelTabkesToInit{other.bwdRelTabkesToInit}, nodeIDVectorPos{other.nodeIDVectorPos},
      propertyLhsPositions{other.propertyLhsPositions}, nodeIDVector{nullptr} {
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

static void writeLhsVector(common::ValueVector* lhsVector, common::ValueVector* rhsVector) {
    KU_ASSERT(lhsVector->state->selVector->selectedSize == 1 &&
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

void NodeInsertExecutor::insert(transaction::Transaction* transaction) {
    for (auto& evaluator : propertyRhsEvaluators) {
        evaluator->evaluate();
    }
    KU_ASSERT(nodeIDVector->state->selVector->selectedSize == 1);
    auto maxNodeOffset = table->insert(transaction, nodeIDVector, propertyRhsVectors);
    for (auto i = 0u; i < propertyLhsVectors.size(); ++i) {
        auto lhsVector = propertyLhsVectors[i];
        auto rhsVector = propertyRhsVectors[i];
        if (lhsVector == nullptr) {
            // No need to project out lhs vector.
            continue;
        }
        KU_ASSERT(lhsVector->state->selVector->selectedSize == 1 &&
                  rhsVector->state->selVector->selectedSize == 1);
        if (lhsVector->dataType.getLogicalTypeID() == LogicalTypeID::SERIAL) {
            // Lhs vector is serial so there is no corresponding rhs vector.
            auto nodeIDPos = nodeIDVector->state->selVector->selectedPositions[0];
            auto lhsPos = lhsVector->state->selVector->selectedPositions[0];
            auto nodeID = nodeIDVector->getValue<nodeID_t>(nodeIDPos);
            lhsVector->setNull(lhsPos, false);
            lhsVector->setValue<int64_t>(lhsPos, nodeID.offset);
        } else {
            writeLhsVector(lhsVector, rhsVector);
        }
    }
    auto nodeGroupIdx = storage::StorageUtils::getNodeGroupIdx(maxNodeOffset);
    for (auto relTable : fwdRelTablesToInit) {
        relTable->resizeColumns(transaction, RelDataDirection::FWD, nodeGroupIdx);
    }
    for (auto relTable : bwdRelTabkesToInit) {
        relTable->resizeColumns(transaction, RelDataDirection::BWD, nodeGroupIdx);
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
      dstNodePos{other.dstNodePos}, propertyLhsPositions{other.propertyLhsPositions},
      srcNodeIDVector{nullptr}, dstNodeIDVector{nullptr} {
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
    auto srcNodeIDPos = srcNodeIDVector->state->selVector->selectedPositions[0];
    auto dstNodeIDPos = dstNodeIDVector->state->selVector->selectedPositions[0];
    if (srcNodeIDVector->isNull(srcNodeIDPos) || dstNodeIDVector->isNull(dstNodeIDPos)) {
        // No need to insert.
        for (auto& lhsVector : propertyLhsVectors) {
            if (lhsVector == nullptr) {
                // No need to project out lhs vector.
                continue;
            }
            auto lhsPos = lhsVector->state->selVector->selectedPositions[0];
            lhsVector->setNull(lhsPos, true);
        }
        return;
    }
    auto offset = relsStatistics.getNextRelOffset(tx, table->getTableID());
    propertyRhsVectors[0]->setValue(0, offset); // internal ID property
    propertyRhsVectors[0]->setNull(0, false);
    for (auto i = 1; i < propertyRhsEvaluators.size(); ++i) {
        propertyRhsEvaluators[i]->evaluate();
    }
    table->insert(tx, srcNodeIDVector, dstNodeIDVector, propertyRhsVectors);
    for (auto i = 0u; i < propertyLhsVectors.size(); ++i) {
        auto lhsVector = propertyLhsVectors[i];
        auto rhsVector = propertyRhsVectors[i];
        if (lhsVector == nullptr) {
            // No need to project out lhs vector.
            continue;
        }
        writeLhsVector(lhsVector, rhsVector);
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
