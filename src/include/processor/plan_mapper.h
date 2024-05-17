#pragma once

#include "common/enums/rel_direction.h"
#include "expression_mapper.h"
#include "planner/operator/logical_plan.h"
#include "processor/operator/result_collector.h"
#include "processor/physical_plan.h"

namespace kuzu {
namespace main {
class ClientContext;
}

namespace binder {
struct BoundDeleteInfo;
}

namespace planner {
struct LogicalSetPropertyInfo;
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
struct ExtraNodeDeleteInfo;

struct BatchInsertSharedState;
struct PartitionerSharedState;

class PlanMapper {
public:
    // Create plan mapper with default mapper context.
    explicit PlanMapper(main::ClientContext* clientContext)
        : expressionMapper{}, clientContext{clientContext}, physicalOperatorID{0} {}

    std::unique_ptr<PhysicalPlan> mapLogicalPlanToPhysical(planner::LogicalPlan* logicalPlan,
        const binder::expression_vector& expressionsToCollect);

private:
    std::unique_ptr<PhysicalOperator> mapOperator(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapScanFile(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapScanFrontier(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapScanInternalID(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapIndexScan(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapEmptyResult(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapUnwind(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapExtend(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapRecursiveExtend(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapPathPropertyProbe(
        planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapFlatten(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapFilter(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapProjection(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapScanNodeProperty(
        planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapSemiMasker(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapHashJoin(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapIntersect(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapCrossProduct(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapMultiplicityReducer(
        planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapNodeLabelFilter(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapLimit(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapMerge(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapAggregate(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapDistinct(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapOrderBy(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapUnionAll(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapAccumulate(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapMarkAccumulate(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapDummyScan(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapInsert(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapSetNodeProperty(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapSetRelProperty(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapDelete(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapDeleteNode(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapDeleteRel(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapCreateTable(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapCreateSequence(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapCopyFrom(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapCopyTo(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapCopyNodeFrom(planner::LogicalOperator* logicalOperator);
    physical_op_vector_t mapCopyRelFrom(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapCopyRdfFrom(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapPartitioner(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapDropTable(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapDropSequence(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapAlter(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapStandaloneCall(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapCommentOn(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapInQueryCall(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapExplain(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapExpressionsScan(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapCreateMacro(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapTransaction(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapExtension(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapExportDatabase(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapImportDatabase(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapAttachDatabase(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapDetachDatabase(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapUseDatabase(planner::LogicalOperator* logicalOperator);

    std::unique_ptr<PhysicalOperator> createCopyRel(
        std::shared_ptr<PartitionerSharedState> partitionerSharedState,
        std::shared_ptr<BatchInsertSharedState> sharedState, planner::LogicalCopyFrom* copyFrom,
        common::RelDataDirection direction, std::vector<common::LogicalType> columnTypes);

    std::unique_ptr<ResultCollector> createResultCollector(common::AccumulateType accumulateType,
        const binder::expression_vector& expressions, planner::Schema* schema,
        std::unique_ptr<PhysicalOperator> prevOperator);

    // Scan fTable with row offset.
    std::unique_ptr<PhysicalOperator> createFTableScan(const binder::expression_vector& exprs,
        std::vector<ft_col_idx_t> colIndices, std::shared_ptr<binder::Expression> offset,
        planner::Schema* schema, std::shared_ptr<FactorizedTable> table, uint64_t maxMorselSize,
        std::unique_ptr<PhysicalOperator> child);
    // Scan fTable without row offset.
    std::unique_ptr<PhysicalOperator> createFTableScan(const binder::expression_vector& exprs,
        std::vector<ft_col_idx_t> colIndices, planner::Schema* schema,
        std::shared_ptr<FactorizedTable> table, uint64_t maxMorselSize,
        std::unique_ptr<PhysicalOperator> child);
    // Scan fTable without row offset.
    // Scan is the leaf operator of physical plan.
    std::unique_ptr<PhysicalOperator> createFTableScan(const binder::expression_vector& exprs,
        std::vector<ft_col_idx_t> colIndices, planner::Schema* schema,
        std::shared_ptr<FactorizedTable> table, uint64_t maxMorselSize);
    // Do not scan anything from table. Serves as a control logic of pull model.
    std::unique_ptr<PhysicalOperator> createEmptyFTableScan(std::shared_ptr<FactorizedTable> table,
        uint64_t maxMorselSize, std::unique_ptr<PhysicalOperator> child);
    // Do not scan anything from table. Serves as a control logic of pull model.
    // Scan is the leaf operator of physical plan.
    std::unique_ptr<PhysicalOperator> createEmptyFTableScan(std::shared_ptr<FactorizedTable> table,
        uint64_t maxMorselSize);
    // Assume scans all columns of table in the same order as given expressions.
    // Scan fTable with row offset.
    std::unique_ptr<PhysicalOperator> createFTableScanAligned(
        const binder::expression_vector& exprs, planner::Schema* schema,
        std::shared_ptr<binder::Expression> offset, std::shared_ptr<FactorizedTable> table,
        uint64_t maxMorselSize, std::unique_ptr<PhysicalOperator> child);
    // Assume scans all columns of table in the same order as given expressions.
    // Scan fTable without row offset.
    std::unique_ptr<PhysicalOperator> createFTableScanAligned(
        const binder::expression_vector& exprs, planner::Schema* schema,
        std::shared_ptr<FactorizedTable> table, uint64_t maxMorselSize,
        std::unique_ptr<PhysicalOperator> child);
    // Assume scans all columns of table in the same order as given expressions.
    // Scan fTable without row offset.
    // Scan is the leaf operator of physical plan.
    std::unique_ptr<PhysicalOperator> createFTableScanAligned(
        const binder::expression_vector& exprs, planner::Schema* schema,
        std::shared_ptr<FactorizedTable> table, uint64_t maxMorselSize);

    std::unique_ptr<HashJoinBuildInfo> createHashBuildInfo(const planner::Schema& buildSideSchema,
        const binder::expression_vector& keys, const binder::expression_vector& payloads);

    std::unique_ptr<PhysicalOperator> createDistinctHashAggregate(
        const binder::expression_vector& keys, const binder::expression_vector& payloads,
        planner::Schema* inSchema, planner::Schema* outSchema,
        std::unique_ptr<PhysicalOperator> prevOperator, const std::string& paramsString);
    std::unique_ptr<PhysicalOperator> createMarkDistinctHashAggregate(
        const binder::expression_vector& keys, const binder::expression_vector& payloads,
        std::shared_ptr<binder::Expression> mark, planner::Schema* inSchema,
        planner::Schema* outSchema, std::unique_ptr<PhysicalOperator> prevOperator,
        const std::string& paramsString);
    std::unique_ptr<PhysicalOperator> createHashAggregate(const binder::expression_vector& keys,
        const binder::expression_vector& payloads, const binder::expression_vector& aggregates,
        std::shared_ptr<binder::Expression> mark, planner::Schema* inSchema,
        planner::Schema* outSchema, std::unique_ptr<PhysicalOperator> prevOperator,
        const std::string& paramsString);

    std::unique_ptr<NodeInsertExecutor> getNodeInsertExecutor(
        const planner::LogicalInsertInfo* info, const planner::Schema& inSchema,
        const planner::Schema& outSchema) const;
    std::unique_ptr<RelInsertExecutor> getRelInsertExecutor(const planner::LogicalInsertInfo* info,
        const planner::Schema& inSchema, const planner::Schema& outSchema) const;
    std::unique_ptr<NodeSetExecutor> getNodeSetExecutor(planner::LogicalSetPropertyInfo* info,
        const planner::Schema& inSchema) const;
    std::unique_ptr<RelSetExecutor> getRelSetExecutor(planner::LogicalSetPropertyInfo* info,
        const planner::Schema& inSchema) const;
    std::unique_ptr<NodeDeleteExecutor> getNodeDeleteExecutor(const binder::BoundDeleteInfo& info,
        const planner::Schema& schema) const;
    std::unique_ptr<RelDeleteExecutor> getRelDeleteExecutor(const binder::BoundDeleteInfo& info,
        const planner::Schema& schema) const;
    ExtraNodeDeleteInfo getExtraNodeDeleteInfo(common::table_id_t tableID, DataPos pkPos) const;

    uint32_t getOperatorID() { return physicalOperatorID++; }

    static void mapSIPJoin(PhysicalOperator* probe);

    static std::vector<DataPos> getDataPos(const binder::expression_vector& expressions,
        const planner::Schema& schema);

    static DataPos getDataPos(const binder::Expression& expression, const planner::Schema& schema) {
        return DataPos(schema.getExpressionPos(expression));
    }

public:
    ExpressionMapper expressionMapper;
    main::ClientContext* clientContext;

private:
    std::unordered_map<planner::LogicalOperator*, PhysicalOperator*> logicalOpToPhysicalOpMap;
    uint32_t physicalOperatorID;
};

} // namespace processor
} // namespace kuzu
