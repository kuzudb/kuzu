#include "binder/copy/bound_table_scan_info.h"
#include "binder/expression/property_expression.h"
#include "catalog/catalog_entry/external_node_table_catalog_entry.h"
#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "main/client_context.h"
#include "planner/operator/logical_hash_join.h"
#include "planner/operator/logical_table_function_call.h"
#include "planner/operator/scan/logical_scan_node_table.h"
#include "planner/planner.h"

using namespace kuzu::common;
using namespace kuzu::catalog;
using namespace kuzu::binder;

namespace kuzu {
namespace planner {

static expression_vector removeInternalIDProperty(const expression_vector& expressions) {
    expression_vector result;
    for (auto expr : expressions) {
        if (expr->constCast<PropertyExpression>().isInternalID()) {
            continue;
        }
        result.push_back(expr);
    }
    return result;
}

void Planner::appendScanNodeTable(const Expression& expr, const expression_vector& properties,
    LogicalPlan& plan) {
    auto& node = expr.constCast<NodeExpression>();
    if (node.hasExternalEntry()) {
        auto entry = node.getSingleEntry()->ptrCast<ExternalNodeTableCatalogEntry>();
        // Scan from physical storage the primary key column
        auto buildPlan = LogicalPlan();
        auto physicalEntry = entry->getPhysicalEntry()->ptrCast<NodeTableCatalogEntry>();
        auto pkName = physicalEntry->getPrimaryKeyName();
        auto pkExpr = node.getPropertyExpression(pkName);
        std::vector<LogicalNodeTableScanInfo> tableScanInfos;
        tableScanInfos.emplace_back(physicalEntry->getTableID(),
            std::vector<column_id_t>{physicalEntry->getColumnID(pkName)});
        appendScanNodeTable(node.getInternalID(), {pkExpr}, tableScanInfos, buildPlan);
        // Scan from external table
        auto externalEntry = node.getExternalEntry();
        auto scanFunc = externalEntry->getScanFunction();
        auto bindInput = function::ScanTableFuncBindInput();
        auto bindData = scanFunc.bindFunc(clientContext, &bindInput);
        auto scanInfo =
            BoundTableScanSourceInfo(scanFunc, std::move(bindData), node.getPropertyExprs());
        appendTableFunctionCall(scanInfo, plan);
        auto& tableFunctionCall = plan.getLastOperator()->cast<LogicalTableFunctionCall>();
        std::vector<bool> columnSkips;
        for (auto i = 0u; i < entry->getNumProperties(); ++i) {
            columnSkips.push_back(
                !propertyExprCollection.contains(expr, entry->getProperty(i).getName()));
        }
        tableFunctionCall.setColumnSkips(columnSkips);
        // Join external table with internal table.
        auto joinCondition = std::make_pair(pkExpr, pkExpr);
        std::vector<binder::expression_pair> joinConditions;
        joinConditions.push_back(joinCondition);
        appendHashJoin(joinConditions, JoinType::INNER, nullptr, plan, buildPlan, plan);
        plan.getLastOperator()->cast<LogicalHashJoin>().vectorizeProbe();
        return;
    }
    appendScanNodeTable(node.getInternalID(), properties, node.getEntries(), plan);
}

static std::vector<column_id_t> getColumnIDs(const TableCatalogEntry& entry,
    const expression_vector& properties) {
    std::vector<column_id_t> columnIDs;
    for (auto& expr : properties) {
        auto& property = expr->constCast<PropertyExpression>();
        if (!property.hasProperty(entry.getTableID())) {
            columnIDs.push_back(INVALID_COLUMN_ID);
            continue;
        }
        columnIDs.push_back(entry.getColumnID(property.getPropertyName()));
    }
    return columnIDs;
}

void Planner::appendScanNodeTable(std::shared_ptr<Expression> nodeID,
    const expression_vector& properties, const std::vector<TableCatalogEntry*>& entries,
    LogicalPlan& plan) {
    auto propertiesToScan_ = removeInternalIDProperty(properties);
    std::vector<LogicalNodeTableScanInfo> tableScanInfos;
    for (auto& entry : entries) {
        tableScanInfos.emplace_back(entry->getTableID(), getColumnIDs(*entry, propertiesToScan_));
    }
    appendScanNodeTable(nodeID, propertiesToScan_, tableScanInfos, plan);
}

void Planner::appendScanNodeTable(std::shared_ptr<Expression> nodeID,
    const expression_vector& properties,
    const std::vector<LogicalNodeTableScanInfo>& tableScanInfos, LogicalPlan& plan) {
    auto scan =
        std::make_shared<LogicalScanNodeTable>(std::move(nodeID), properties, tableScanInfos);
    scan->computeFactorizedSchema();
    plan.setCardinality(cardinalityEstimator.estimateScanNode(scan.get()));
    plan.setLastOperator(std::move(scan));
}

} // namespace planner
} // namespace kuzu
