#include "processor/operator/update/insert_manager.h"

namespace kuzu {
namespace processor {

NodeTableInsertManager::NodeTableInsertManager(const NodeTableInsertManager& other)
    : table{other.table}, relTablesToInit{other.relTablesToInit}, outNodeIDVectorPos{
                                                                      other.outNodeIDVectorPos} {
    if (other.primaryKeyEvaluator != nullptr) {
        primaryKeyEvaluator = other.primaryKeyEvaluator->clone();
    }
}

void NodeTableInsertManager::init(ResultSet* resultSet, ExecutionContext* context) {
    if (primaryKeyEvaluator != nullptr) {
        primaryKeyEvaluator->init(*resultSet, context->memoryManager);
        primaryKeyVector = primaryKeyEvaluator->resultVector.get();
    }
    outNodeIDVector = resultSet->getValueVector(outNodeIDVectorPos).get();
}

void NodeTableInsertManager::insert(transaction::Transaction* transaction) {
    auto offset = table->addNode(transaction);
    table->setPropertiesToNull(offset);
    if (primaryKeyVector != nullptr) {
        primaryKeyEvaluator->evaluate();
        table->insertPK(offset, primaryKeyVector);
    }
    common::nodeID_t nodeID{offset, table->getTableID()};
    assert(outNodeIDVector->state->selVector->selectedSize == 1);
    auto pos = outNodeIDVector->state->selVector->selectedPositions[0];
    outNodeIDVector->setValue<common::nodeID_t>(pos, nodeID);
    for (auto& relTable : relTablesToInit) {
        relTable->initEmptyRelsForNewNode(nodeID);
    }
}

RelTableInsertManager::RelTableInsertManager(const RelTableInsertManager& other)
    : relsStatistics{other.relsStatistics}, table{other.table}, srcNodePos{other.srcNodePos},
      dstNodePos{other.dstNodePos}, srcNodeTableID{other.srcNodeTableID},
      dstNodeTableID{other.dstNodeTableID} {
    for (auto& evaluator : other.evaluators) {
        evaluators.push_back(evaluator->clone());
    }
}

void RelTableInsertManager::init(ResultSet* resultSet, ExecutionContext* context) {
    srcNodeIDVector = resultSet->getValueVector(srcNodePos).get();
    dstNodeIDVector = resultSet->getValueVector(dstNodePos).get();
    for (auto& evaluator : evaluators) {
        evaluator->init(*resultSet, context->memoryManager);
        propertyVectors.push_back(evaluator->resultVector.get());
    }
}

void RelTableInsertManager::insert(transaction::Transaction* tx) {
    auto offset = relsStatistics.getNextRelOffset(tx, table->getRelTableID());
    propertyVectors[0]->setValue(0, offset);
    propertyVectors[0]->setNull(0, false);
    for (auto i = 1; i < evaluators.size(); ++i) {
        evaluators[i]->evaluate();
    }
    table->insertRel(srcNodeIDVector, dstNodeIDVector, propertyVectors);
    relsStatistics.updateNumRelsByValue(table->getRelTableID(), 1);
}

} // namespace processor
} // namespace kuzu
