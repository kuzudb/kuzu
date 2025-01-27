#pragma once

#include "planner/operator/logical_operator.h"
#include "planner/operator/logical_plan.h"
#include "processor/operator/result_collector.h"
#include "processor/physical_plan.h"

namespace kuzu {
namespace main {
class ClientContext;
}

namespace binder {
struct BoundDeleteInfo;
struct BoundSetPropertyInfo;
} // namespace binder

namespace catalog {
class TableCatalogEntry;
}

namespace planner {
struct LogicalInsertInfo;
class LogicalCopyFrom;
} // namespace planner

namespace processor {

class HashJoinBuildInfo;
struct AggregateInfo;
class NodeInsertExecutor;
class RelInsertExecutor;
class NodeSetExecutor;
class RelSetExecutor;
class NodeDeleteExecutor;
class RelDeleteExecutor;
struct NodeTableDeleteInfo;
struct NodeTableSetInfo;
struct RelTableSetInfo;

struct BatchInsertSharedState;
struct PartitionerSharedState;

class PlanMapper {
public:
    explicit PlanMapper(main::ClientContext* clientContext)
        : clientContext{clientContext}, physicalOperatorID{0} {}

    std::unique_ptr<PhysicalPlan> mapLogicalPlanToPhysical(const planner::LogicalPlan* logicalPlan,
        const binder::expression_vector& expressionsToCollect);

private:
    std::unique_ptr<PhysicalOperator> mapOperator(const planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapAccumulate(
        const planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapAggregate(const planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapAlter(const planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapAttachDatabase(
        const planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapCopyFrom(const planner::LogicalOperator* logicalOperator);
    physical_op_vector_t mapCopyHNSWIndexFrom(const planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapCopyNodeFrom(
        const planner::LogicalOperator* logicalOperator);
    physical_op_vector_t mapCopyRelFrom(const planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapCopyTo(const planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapCreateMacro(
        const planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapCreateSequence(
        const planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapCreateTable(
        const planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapCreateType(
        const planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapCrossProduct(
        const planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapDelete(const planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapDeleteNode(
        const planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapDeleteRel(const planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapDetachDatabase(
        const planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapDistinct(const planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapDrop(const planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapDummyScan(const planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapEmptyResult(
        const planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapExplain(const planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapExpressionsScan(
        const planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapExtend(const planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapExtension(const planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapExportDatabase(
        const planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapFilter(const planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapFlatten(const planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapGDSCall(const planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapHashJoin(const planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapImportDatabase(
        const planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapIndexLookup(
        const planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapIntersect(const planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapInsert(const planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapLimit(const planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapMerge(const planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapMultiplicityReducer(
        const planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapNodeLabelFilter(
        const planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapOrderBy(const planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapPartitioner(
        const planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapPathPropertyProbe(
        const planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapProjection(
        const planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapRecursiveExtend(
        const planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapScanNodeTable(
        const planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapSemiMasker(
        const planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapSetProperty(
        const planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapSetNodeProperty(
        const planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapSetRelProperty(
        const planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapStandaloneCall(
        const planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapTableFunctionCall(
        const planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapTransaction(
        const planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapUnionAll(const planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapUnwind(const planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapUseDatabase(
        const planner::LogicalOperator* logicalOperator);

    std::unique_ptr<ResultCollector> createResultCollector(common::AccumulateType accumulateType,
        const binder::expression_vector& expressions, planner::Schema* schema,
        std::unique_ptr<PhysicalOperator> prevOperator);

    // Scan fTable
    std::unique_ptr<PhysicalOperator> createFTableScan(const binder::expression_vector& exprs,
        std::vector<ft_col_idx_t> colIndices, const planner::Schema* schema,
        std::shared_ptr<FactorizedTable> table, uint64_t maxMorselSize,
        physical_op_vector_t children);
    // Scan is the leaf operator of physical plan.
    std::unique_ptr<PhysicalOperator> createFTableScan(const binder::expression_vector& exprs,
        const std::vector<ft_col_idx_t>& colIndices, const planner::Schema* schema,
        std::shared_ptr<FactorizedTable> table, uint64_t maxMorselSize);
    // Do not scan anything from table. Serves as a control logic of pull model.
    std::unique_ptr<PhysicalOperator> createEmptyFTableScan(std::shared_ptr<FactorizedTable> table,
        uint64_t maxMorselSize, physical_op_vector_t children);
    std::unique_ptr<PhysicalOperator> createEmptyFTableScan(std::shared_ptr<FactorizedTable> table,
        uint64_t maxMorselSize, std::unique_ptr<PhysicalOperator> child);
    // Do not scan anything from table. Serves as a control logic of pull model.
    // Scan is the leaf operator of physical plan.
    std::unique_ptr<PhysicalOperator> createEmptyFTableScan(std::shared_ptr<FactorizedTable> table,
        uint64_t maxMorselSize);
    // Assume scans all columns of table in the same order as given expressions.
    std::unique_ptr<PhysicalOperator> createFTableScanAligned(
        const binder::expression_vector& exprs, const planner::Schema* schema,
        std::shared_ptr<FactorizedTable> table, uint64_t maxMorselSize,
        physical_op_vector_t children);
    // Assume scans all columns of table in the same order as given expressions.
    // Scan fTable without row offset.
    // Scan is the leaf operator of physical plan.
    std::unique_ptr<PhysicalOperator> createFTableScanAligned(
        const binder::expression_vector& exprs, const planner::Schema* schema,
        std::shared_ptr<FactorizedTable> table, uint64_t maxMorselSize);

    static std::unique_ptr<HashJoinBuildInfo> createHashBuildInfo(
        const planner::Schema& buildSideSchema, const binder::expression_vector& keys,
        const binder::expression_vector& payloads);

    std::unique_ptr<PhysicalOperator> createDistinctHashAggregate(
        const binder::expression_vector& keys, const binder::expression_vector& payloads,
        planner::Schema* inSchema, planner::Schema* outSchema,
        std::unique_ptr<PhysicalOperator> prevOperator);
    std::unique_ptr<PhysicalOperator> createHashAggregate(const binder::expression_vector& keys,
        const binder::expression_vector& payloads, const binder::expression_vector& aggregates,
        planner::Schema* inSchema, planner::Schema* outSchema,
        std::unique_ptr<PhysicalOperator> prevOperator);

    NodeInsertExecutor getNodeInsertExecutor(const planner::LogicalInsertInfo* boundInfo,
        const planner::Schema& inSchema, const planner::Schema& outSchema) const;
    RelInsertExecutor getRelInsertExecutor(const planner::LogicalInsertInfo* boundInfo,
        const planner::Schema& inSchema, const planner::Schema& outSchema) const;
    std::unique_ptr<NodeSetExecutor> getNodeSetExecutor(
        const binder::BoundSetPropertyInfo& boundInfo, const planner::Schema& schema) const;
    std::unique_ptr<RelSetExecutor> getRelSetExecutor(const binder::BoundSetPropertyInfo& boundInfo,
        const planner::Schema& schema) const;
    std::unique_ptr<NodeDeleteExecutor> getNodeDeleteExecutor(
        const binder::BoundDeleteInfo& boundInfo, const planner::Schema& schema) const;
    std::unique_ptr<RelDeleteExecutor> getRelDeleteExecutor(
        const binder::BoundDeleteInfo& boundInfo, const planner::Schema& schema) const;
    NodeTableDeleteInfo getNodeTableDeleteInfo(const catalog::TableCatalogEntry& entry,
        DataPos pkPos) const;
    NodeTableSetInfo getNodeTableSetInfo(const catalog::TableCatalogEntry& entry,
        const binder::Expression& expr) const;
    RelTableSetInfo getRelTableSetInfo(const catalog::TableCatalogEntry& entry,
        const binder::Expression& expr) const;
    uint32_t getOperatorID() { return physicalOperatorID++; }

    static void mapSIPJoin(PhysicalOperator* joinRoot);

    static std::vector<DataPos> getDataPos(const binder::expression_vector& expressions,
        const planner::Schema& schema);

    static DataPos getDataPos(const binder::Expression& expression, const planner::Schema& schema) {
        return DataPos(schema.getExpressionPos(expression));
    }

public:
    main::ClientContext* clientContext;

private:
    std::unordered_map<const planner::LogicalOperator*, PhysicalOperator*> logicalOpToPhysicalOpMap;
    uint32_t physicalOperatorID;
};

} // namespace processor
} // namespace kuzu
