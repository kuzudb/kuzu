#include "binder/expression/node_expression.h"
#include "binder/expression/rel_expression.h"
#include "catalog/catalog_entry/node_table_catalog_entry.h"
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

static void checkDetachDeleteRelDirection(const RelTable* relTable, const NodeTable* nodeTable) {
    if (relTable->getStorageDirections().size() < NUM_REL_DIRECTIONS) {
        throw RuntimeException(stringFormat(
            "Cannot detach delete from node table '{}' as it has connected edges in rel "
            "table '{}' (detach delete only supports deleting from rel tables with "
            "storage direction 'both').",
            nodeTable->getTableName(), relTable->getTableName()));
    }
}

table_id_set_t getFwdRelTableIDs(table_id_t nodeTableID, Catalog* catalog,
    transaction::Transaction* transaction) {
    table_id_set_t result;
    for (const auto& relEntry : catalog->getRelTableEntries(transaction)) {
        if (relEntry->getSrcTableID() == nodeTableID) {
            result.insert(relEntry->getTableID());
        }
    }
    return result;
}

table_id_set_t getBwdRelTableIDs(table_id_t nodeTableID, Catalog* catalog,
    transaction::Transaction* transaction) {
    table_id_set_t result;
    for (const auto& relEntry : catalog->getRelTableEntries(transaction)) {
        if (relEntry->getDstTableID() == nodeTableID) {
            result.insert(relEntry->getTableID());
        }
    }
    return result;
}

NodeTableDeleteInfo PlanMapper::getNodeTableDeleteInfo(const TableCatalogEntry& entry,
    DataPos pkPos) const {
    auto storageManager = clientContext->getStorageManager();
    auto catalog = clientContext->getCatalog();
    auto transaction = clientContext->getTransaction();
    auto tableID = entry.getTableID();
    auto table = storageManager->getTable(tableID)->ptrCast<NodeTable>();
    std::unordered_set<RelTable*> fwdRelTables;
    std::unordered_set<RelTable*> bwdRelTables;
    auto& nodeEntry = entry.constCast<NodeTableCatalogEntry>();
    for (auto id : getFwdRelTableIDs(nodeEntry.getTableID(), catalog, transaction)) {
        auto* relTable = storageManager->getTable(id)->ptrCast<RelTable>();
        checkDetachDeleteRelDirection(relTable, table);
        fwdRelTables.insert(relTable);
    }
    for (auto id : getBwdRelTableIDs(nodeEntry.getTableID(), catalog, transaction)) {
        auto* relTable = storageManager->getTable(id)->ptrCast<RelTable>();
        checkDetachDeleteRelDirection(relTable, table);
        bwdRelTables.insert(relTable);
    }
    return NodeTableDeleteInfo(table, std::move(fwdRelTables), std::move(bwdRelTables), pkPos);
}

std::unique_ptr<NodeDeleteExecutor> PlanMapper::getNodeDeleteExecutor(
    const BoundDeleteInfo& boundInfo, const Schema& schema) const {
    KU_ASSERT(boundInfo.tableType == TableType::NODE);
    auto& node = boundInfo.pattern->constCast<NodeExpression>();
    auto nodeIDPos = getDataPos(*node.getInternalID(), schema);
    auto info = NodeDeleteInfo(boundInfo.deleteType, nodeIDPos);
    if (node.isEmpty()) {
        return std::make_unique<EmptyNodeDeleteExecutor>(std::move(info));
    }
    if (node.isMultiLabeled()) {
        table_id_map_t<NodeTableDeleteInfo> tableInfos;
        for (auto entry : node.getEntries()) {
            auto tableID = entry->getTableID();
            auto pkPos = getDataPos(*node.getPrimaryKey(tableID), schema);
            tableInfos.insert({tableID, getNodeTableDeleteInfo(*entry, pkPos)});
        }
        return std::make_unique<MultiLabelNodeDeleteExecutor>(std::move(info),
            std::move(tableInfos));
    }
    auto pkPos = getDataPos(*node.getPrimaryKey(node.getSingleEntry()->getTableID()), schema);
    auto extraInfo = getNodeTableDeleteInfo(*node.getSingleEntry(), pkPos);
    return std::make_unique<SingleLabelNodeDeleteExecutor>(std::move(info), std::move(extraInfo));
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapDelete(const LogicalOperator* logicalOperator) {
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

std::unique_ptr<PhysicalOperator> PlanMapper::mapDeleteNode(
    const LogicalOperator* logicalOperator) {
    auto delete_ = logicalOperator->constPtrCast<LogicalDelete>();
    auto inSchema = delete_->getChild(0)->getSchema();
    auto prevOperator = mapOperator(logicalOperator->getChild(0).get());
    std::vector<std::unique_ptr<NodeDeleteExecutor>> executors;
    for (auto& info : delete_->getInfos()) {
        executors.push_back(getNodeDeleteExecutor(info, *inSchema));
    }
    expression_vector patterns;
    for (auto& info : delete_->getInfos()) {
        patterns.push_back(info.pattern);
    }
    auto printInfo =
        std::make_unique<DeleteNodePrintInfo>(patterns, delete_->getInfos()[0].deleteType);
    return std::make_unique<DeleteNode>(std::move(executors), std::move(prevOperator),
        getOperatorID(), std::move(printInfo));
}

std::unique_ptr<RelDeleteExecutor> PlanMapper::getRelDeleteExecutor(
    const BoundDeleteInfo& boundInfo, const Schema& schema) const {
    auto& rel = boundInfo.pattern->constCast<RelExpression>();
    if (rel.isEmpty()) {
        return std::make_unique<EmptyRelDeleteExecutor>();
    }
    auto relIDPos = getDataPos(*rel.getInternalIDProperty(), schema);
    auto srcNodeIDPos = getDataPos(*rel.getSrcNode()->getInternalID(), schema);
    auto dstNodeIDPos = getDataPos(*rel.getDstNode()->getInternalID(), schema);
    auto info = RelDeleteInfo(srcNodeIDPos, dstNodeIDPos, relIDPos);
    auto storageManager = clientContext->getStorageManager();
    if (rel.isMultiLabeled()) {
        table_id_map_t<RelTable*> tableIDToTableMap;
        for (auto entry : rel.getEntries()) {
            auto tableID = entry->getTableID();
            auto table = storageManager->getTable(tableID)->ptrCast<RelTable>();
            tableIDToTableMap.insert({tableID, table});
        }
        return std::make_unique<MultiLabelRelDeleteExecutor>(std::move(tableIDToTableMap),
            std::move(info));
    }
    auto table = storageManager->getTable(rel.getSingleEntry()->getTableID())->ptrCast<RelTable>();
    return std::make_unique<SingleLabelRelDeleteExecutor>(table, std::move(info));
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapDeleteRel(const LogicalOperator* logicalOperator) {
    auto delete_ = logicalOperator->constPtrCast<LogicalDelete>();
    auto inSchema = delete_->getChild(0)->getSchema();
    auto prevOperator = mapOperator(logicalOperator->getChild(0).get());
    std::vector<std::unique_ptr<RelDeleteExecutor>> executors;
    for (auto& info : delete_->getInfos()) {
        executors.push_back(getRelDeleteExecutor(info, *inSchema));
    }
    expression_vector patterns;
    for (auto& info : delete_->getInfos()) {
        patterns.push_back(info.pattern);
    }
    auto printInfo = std::make_unique<DeleteRelPrintInfo>(patterns);
    return std::make_unique<DeleteRel>(std::move(executors), std::move(prevOperator),
        getOperatorID(), std::move(printInfo));
}

} // namespace processor
} // namespace kuzu
