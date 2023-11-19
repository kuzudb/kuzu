#include "planner/operator/persistent/logical_delete.h"
#include "processor/operator/persistent/delete.h"
#include "processor/plan_mapper.h"

using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::planner;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

static std::unique_ptr<NodeDeleteExecutor> getNodeDeleteExecutor(catalog::Catalog* catalog,
    StorageManager& storageManager, LogicalDeleteNodeInfo* info, const Schema& inSchema) {
    auto nodeIDPos = DataPos(inSchema.getExpressionPos(*info->node->getInternalID()));
    if (info->node->isMultiLabeled()) {
        std::unordered_map<table_id_t, NodeTable*> tableIDToTableMap;
        std::unordered_map<table_id_t, std::unordered_set<RelTable*>> tableIDToFwdRelTablesMap;
        std::unordered_map<table_id_t, std::unordered_set<RelTable*>> tableIDToBwdRelTablesMap;
        for (auto tableID : info->node->getTableIDs()) {
            auto tableSchema = catalog->getReadOnlyVersion()->getTableSchema(tableID);
            auto nodeTableSchema =
                ku_dynamic_cast<catalog::TableSchema*, catalog::NodeTableSchema*>(tableSchema);
            auto table = storageManager.getNodeTable(tableID);
            auto fwdRelTableIDs = nodeTableSchema->getFwdRelTableIDSet();
            auto bwdRelTableIDs = nodeTableSchema->getBwdRelTableIDSet();
            std::unordered_set<RelTable*> fwdRelTables;
            std::unordered_set<RelTable*> bwdRelTables;
            for (auto relTableID : fwdRelTableIDs) {
                fwdRelTables.insert(storageManager.getRelTable(relTableID));
            }
            for (auto relTableID : bwdRelTableIDs) {
                bwdRelTables.insert(storageManager.getRelTable(relTableID));
            }
            tableIDToTableMap.insert({tableID, table});
            tableIDToFwdRelTablesMap[tableID] = fwdRelTables;
            tableIDToBwdRelTablesMap[tableID] = bwdRelTables;
        }
        return std::make_unique<MultiLabelNodeDeleteExecutor>(std::move(tableIDToTableMap),
            std::move(tableIDToFwdRelTablesMap), std::move(tableIDToBwdRelTablesMap),
            info->deleteType, nodeIDPos);
    } else {
        auto table = storageManager.getNodeTable(info->node->getSingleTableID());
        auto tableSchema =
            catalog->getReadOnlyVersion()->getTableSchema(info->node->getSingleTableID());
        auto nodeTableSchema =
            ku_dynamic_cast<catalog::TableSchema*, catalog::NodeTableSchema*>(tableSchema);
        auto fwdRelTableIDs = nodeTableSchema->getFwdRelTableIDSet();
        auto bwdRelTableIDs = nodeTableSchema->getBwdRelTableIDSet();
        std::unordered_set<RelTable*> fwdRelTables;
        std::unordered_set<RelTable*> bwdRelTables;
        for (auto tableID : fwdRelTableIDs) {
            fwdRelTables.insert(storageManager.getRelTable(tableID));
        }
        for (auto tableID : bwdRelTableIDs) {
            bwdRelTables.insert(storageManager.getRelTable(tableID));
        }
        return std::make_unique<SingleLabelNodeDeleteExecutor>(
            table, std::move(fwdRelTables), std::move(bwdRelTables), info->deleteType, nodeIDPos);
    }
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapDeleteNode(LogicalOperator* logicalOperator) {
    auto logicalDeleteNode = (LogicalDeleteNode*)logicalOperator;
    auto inSchema = logicalDeleteNode->getChild(0)->getSchema();
    auto prevOperator = mapOperator(logicalOperator->getChild(0).get());
    std::vector<std::unique_ptr<NodeDeleteExecutor>> executors;
    for (auto deleteInfo : logicalDeleteNode->getInfos()) {
        executors.push_back(getNodeDeleteExecutor(catalog, storageManager, deleteInfo, *inSchema));
    }
    return std::make_unique<DeleteNode>(std::move(executors), std::move(prevOperator),
        getOperatorID(), logicalDeleteNode->getExpressionsForPrinting());
}

static std::unique_ptr<RelDeleteExecutor> getRelDeleteExecutor(
    storage::StorageManager& storageManager, const binder::RelExpression& rel,
    const Schema& inSchema) {
    auto srcNodePos = DataPos(inSchema.getExpressionPos(*rel.getSrcNode()->getInternalID()));
    auto dstNodePos = DataPos(inSchema.getExpressionPos(*rel.getDstNode()->getInternalID()));
    auto relIDPos = DataPos(inSchema.getExpressionPos(*rel.getInternalIDProperty()));
    if (rel.isMultiLabeled()) {
        std::unordered_map<table_id_t, storage::RelTable*> tableIDToTableMap;
        for (auto tableID : rel.getTableIDs()) {
            auto table = storageManager.getRelTable(tableID);
            tableIDToTableMap.insert({tableID, table});
        }
        return std::make_unique<MultiLabelRelDeleteExecutor>(
            std::move(tableIDToTableMap), srcNodePos, dstNodePos, relIDPos);
    } else {
        auto table = storageManager.getRelTable(rel.getSingleTableID());
        return std::make_unique<SingleLabelRelDeleteExecutor>(
            table, srcNodePos, dstNodePos, relIDPos);
    }
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapDeleteRel(LogicalOperator* logicalOperator) {
    auto logicalDeleteRel = (LogicalDeleteRel*)logicalOperator;
    auto inSchema = logicalDeleteRel->getChild(0)->getSchema();
    auto prevOperator = mapOperator(logicalOperator->getChild(0).get());
    std::vector<std::unique_ptr<RelDeleteExecutor>> Executors;
    for (auto& rel : logicalDeleteRel->getRelsRef()) {
        Executors.push_back(getRelDeleteExecutor(storageManager, *rel, *inSchema));
    }
    return std::make_unique<DeleteRel>(std::move(Executors), std::move(prevOperator),
        getOperatorID(), logicalOperator->getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu
