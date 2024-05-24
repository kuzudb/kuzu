#include "binder/expression/node_expression.h"
#include "binder/expression/rel_expression.h"
#include "planner/operator/persistent/logical_delete.h"
#include "processor/operator/persistent/delete.h"
#include "processor/plan_mapper.h"
#include "storage/storage_manager.h"

using namespace kuzu::binder;
using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::planner;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

ExtraNodeDeleteInfo PlanMapper::getExtraNodeDeleteInfo(table_id_t tableID, DataPos pkPos) const {
    auto sm = clientContext->getStorageManager();
    auto catalog = clientContext->getCatalog();
    auto table = sm->getTable(tableID)->ptrCast<NodeTable>();
    auto entry = catalog->getTableCatalogEntry(clientContext->getTx(), tableID);
    auto nodeEntry = entry->constPtrCast<NodeTableCatalogEntry>();
    std::unordered_set<RelTable*> fwdRelTables;
    std::unordered_set<RelTable*> bwdRelTables;
    for (auto id : nodeEntry->getFwdRelTableIDSet()) {
        fwdRelTables.insert(sm->getTable(id)->ptrCast<RelTable>());
    }
    for (auto id : nodeEntry->getBwdRelTableIDSet()) {
        bwdRelTables.insert(sm->getTable(id)->ptrCast<RelTable>());
    }
    return ExtraNodeDeleteInfo(table, std::move(fwdRelTables), std::move(bwdRelTables), pkPos);
}

std::unique_ptr<NodeDeleteExecutor> PlanMapper::getNodeDeleteExecutor(
    const BoundDeleteInfo& boundInfo, const Schema& schema) const {
    KU_ASSERT(boundInfo.tableType == TableType::NODE);
    auto& node = boundInfo.pattern->constCast<NodeExpression>();
    auto nodeIDPos = getDataPos(*node.getInternalID(), schema);
    auto info = NodeDeleteInfo(boundInfo.deleteType, nodeIDPos);
    if (node.isMultiLabeled()) {
        common::table_id_map_t<ExtraNodeDeleteInfo> extraInfos;
        for (auto id : node.getTableIDs()) {
            auto pkPos = getDataPos(*node.getPrimaryKey(id), schema);
            extraInfos.insert({id, getExtraNodeDeleteInfo(id, pkPos)});
        }
        return std::make_unique<MultiLabelNodeDeleteExecutor>(std::move(info),
            std::move(extraInfos));
    }
    auto pkPos = getDataPos(*node.getPrimaryKey(node.getSingleTableID()), schema);
    auto extraInfo = getExtraNodeDeleteInfo(node.getSingleTableID(), pkPos);
    return std::make_unique<SingleLabelNodeDeleteExecutor>(std::move(info), std::move(extraInfo));
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapDelete(planner::LogicalOperator* logicalOperator) {
    auto delete_ = logicalOperator->constPtrCast<LogicalDelete>();
    switch (delete_->getTableType()) {
    case TableType::NODE: {
        return mapDeleteNode(logicalOperator);
    }
    case TableType::REL: {
        return mapDeleteRel(logicalOperator);
    }
    default:
        KU_UNREACHABLE;
    }
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapDeleteNode(LogicalOperator* logicalOperator) {
    auto delete_ = logicalOperator->constPtrCast<LogicalDelete>();
    auto inSchema = delete_->getChild(0)->getSchema();
    auto prevOperator = mapOperator(logicalOperator->getChild(0).get());
    std::vector<std::unique_ptr<NodeDeleteExecutor>> executors;
    for (auto& info : delete_->getInfos()) {
        executors.push_back(getNodeDeleteExecutor(info, *inSchema));
    }
    return std::make_unique<DeleteNode>(std::move(executors), std::move(prevOperator),
        getOperatorID(), delete_->getExpressionsForPrinting());
}

std::unique_ptr<RelDeleteExecutor> PlanMapper::getRelDeleteExecutor(const BoundDeleteInfo& info,
    const Schema& schema) const {
    auto sm = clientContext->getStorageManager();
    auto& rel = info.pattern->constCast<RelExpression>();
    auto srcNodePos = getDataPos(*rel.getSrcNode()->getInternalID(), schema);
    auto dstNodePos = getDataPos(*rel.getDstNode()->getInternalID(), schema);
    auto relIDPos = getDataPos(*rel.getInternalIDProperty(), schema);
    if (rel.isMultiLabeled()) {
        common::table_id_map_t<storage::RelTable*> tableIDToTableMap;
        for (auto tableID : rel.getTableIDs()) {
            auto table = sm->getTable(tableID)->ptrCast<RelTable>();
            tableIDToTableMap.insert({tableID, table});
        }
        return std::make_unique<MultiLabelRelDeleteExecutor>(std::move(tableIDToTableMap),
            srcNodePos, dstNodePos, relIDPos);
    }
    auto table = sm->getTable(rel.getSingleTableID())->ptrCast<RelTable>();
    return std::make_unique<SingleLabelRelDeleteExecutor>(table, srcNodePos, dstNodePos, relIDPos);
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapDeleteRel(LogicalOperator* logicalOperator) {
    auto delete_ = logicalOperator->constPtrCast<LogicalDelete>();
    auto inSchema = delete_->getChild(0)->getSchema();
    auto prevOperator = mapOperator(logicalOperator->getChild(0).get());
    std::vector<std::unique_ptr<RelDeleteExecutor>> executors;
    for (auto& info : delete_->getInfos()) {
        executors.push_back(getRelDeleteExecutor(info, *inSchema));
    }
    return std::make_unique<DeleteRel>(std::move(executors), std::move(prevOperator),
        getOperatorID(), logicalOperator->getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu
