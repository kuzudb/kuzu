#pragma once

#include "common/enums/rel_direction.h"
#include "expression_mapper.h"
#include "function/aggregate_function.h"
#include "planner/operator/logical_plan.h"
#include "processor/operator/result_collector.h"
#include "processor/physical_plan.h"
#include "storage/storage_manager.h"

namespace kuzu {
namespace planner {
struct LogicalSetPropertyInfo;
struct LogicalInsertNodeInfo;
struct LogicalInsertRelInfo;
class LogicalCopyFrom;
} // namespace planner

namespace processor {

struct HashJoinBuildInfo;
struct AggregateInputInfo;
class NodeInsertExecutor;
class RelInsertExecutor;
class NodeSetExecutor;
class RelSetExecutor;
class CopyRelSharedState;
class PartitionerSharedState;

class PlanMapper {
public:
    // Create plan mapper with default mapper context.
    PlanMapper(storage::StorageManager& storageManager, storage::MemoryManager* memoryManager,
        catalog::Catalog* catalog)
        : storageManager{storageManager}, memoryManager{memoryManager},
          expressionMapper{}, catalog{catalog}, physicalOperatorID{0} {}

    std::unique_ptr<PhysicalPlan> mapLogicalPlanToPhysical(
        planner::LogicalPlan* logicalPlan, const binder::expression_vector& expressionsToCollect);

private:
    std::unique_ptr<PhysicalOperator> mapOperator(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapScanFile(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapScanFrontier(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapScanInternalID(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapFillTableID(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapIndexScan(planner::LogicalOperator* logicalOperator);
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
    std::unique_ptr<PhysicalOperator> mapDummyScan(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapInsertNode(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapInsertRel(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapSetNodeProperty(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapSetRelProperty(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapDeleteNode(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapDeleteRel(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapCreateTable(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapCreateNodeTable(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapCreateRelTable(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapCreateRelTableGroup(
        planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapCreateRdfGraph(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapCopyFrom(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapCopyTo(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapCopyNodeFrom(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapCopyRelFrom(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapPartitioner(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapDropTable(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapAlter(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapRenameTable(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapAddProperty(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapDropProperty(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapRenameProperty(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapStandaloneCall(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapCommentOn(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapInQueryCall(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapExplain(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapExpressionsScan(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapCreateMacro(planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapTransaction(planner::LogicalOperator* logicalOperator);

    std::unique_ptr<PhysicalOperator> createCopyRel(
        std::shared_ptr<PartitionerSharedState> partitionerSharedState,
        std::shared_ptr<CopyRelSharedState> sharedState, planner::LogicalCopyFrom* copyFrom,
        common::RelDataDirection direction);

    std::unique_ptr<ResultCollector> createResultCollector(common::AccumulateType accumulateType,
        const binder::expression_vector& expressions, planner::Schema* schema,
        std::unique_ptr<PhysicalOperator> prevOperator);
    std::unique_ptr<PhysicalOperator> createFactorizedTableScan(
        const binder::expression_vector& expressions, std::vector<ft_col_idx_t> colIndices,
        planner::Schema* schema, std::shared_ptr<FactorizedTable> table, uint64_t maxMorselSize,
        std::unique_ptr<PhysicalOperator> prevOperator);
    // Assume scans all columns of table in the same order as given expressions.
    std::unique_ptr<PhysicalOperator> createFactorizedTableScanAligned(
        const binder::expression_vector& expressions, planner::Schema* schema,
        std::shared_ptr<FactorizedTable> table, uint64_t maxMorselSize,
        std::unique_ptr<PhysicalOperator> prevOperator);
    std::unique_ptr<HashJoinBuildInfo> createHashBuildInfo(const planner::Schema& buildSideSchema,
        const binder::expression_vector& keys, const binder::expression_vector& payloads);
    std::unique_ptr<PhysicalOperator> createHashAggregate(
        const binder::expression_vector& keyExpressions,
        const binder::expression_vector& dependentKeyExpressions,
        std::vector<std::unique_ptr<function::AggregateFunction>> aggregateFunctions,
        std::vector<std::unique_ptr<AggregateInputInfo>> aggregateInputInfos,
        std::vector<DataPos> aggregatesOutputPos, planner::Schema* inSchema,
        planner::Schema* outSchema, std::unique_ptr<PhysicalOperator> prevOperator,
        const std::string& paramsString);

    std::unique_ptr<NodeInsertExecutor> getNodeInsertExecutor(planner::LogicalInsertNodeInfo* info,
        const planner::Schema& inSchema, const planner::Schema& outSchema) const;
    std::unique_ptr<RelInsertExecutor> getRelInsertExecutor(planner::LogicalInsertRelInfo* info,
        const planner::Schema& inSchema, const planner::Schema& outSchema);
    std::unique_ptr<NodeSetExecutor> getNodeSetExecutor(
        planner::LogicalSetPropertyInfo* info, const planner::Schema& inSchema);
    std::unique_ptr<RelSetExecutor> getRelSetExecutor(
        planner::LogicalSetPropertyInfo* info, const planner::Schema& inSchema);

    std::shared_ptr<FactorizedTable> getSingleStringColumnFTable();

    inline uint32_t getOperatorID() { return physicalOperatorID++; }

    static void mapSIPJoin(PhysicalOperator* probe);

    static std::vector<DataPos> getExpressionsDataPos(
        const binder::expression_vector& expressions, const planner::Schema& schema);

    static inline DataPos getDataPos(
        const binder::Expression& expression, const planner::Schema& schema) {
        return DataPos(schema.getExpressionPos(expression));
    }

public:
    storage::StorageManager& storageManager;
    storage::MemoryManager* memoryManager;
    ExpressionMapper expressionMapper;
    catalog::Catalog* catalog;

private:
    std::unordered_map<planner::LogicalOperator*, PhysicalOperator*> logicalOpToPhysicalOpMap;
    uint32_t physicalOperatorID;
};

} // namespace processor
} // namespace kuzu
