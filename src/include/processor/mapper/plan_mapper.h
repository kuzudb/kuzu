#pragma once

#include "binder/expression/node_expression.h"
#include "common/statement_type.h"
#include "planner/logical_plan/logical_plan.h"
#include "processor/mapper/expression_mapper.h"
#include "processor/operator/result_collector.h"
#include "processor/physical_plan.h"
#include "storage/storage_manager.h"
#include "storage/store/nodes_statistics_and_deleted_ids.h"

namespace kuzu {
namespace processor {

struct BuildDataInfo;

class PlanMapper {
public:
    // Create plan mapper with default mapper context.
    PlanMapper(storage::StorageManager& storageManager, storage::MemoryManager* memoryManager,
        catalog::Catalog* catalog)
        : storageManager{storageManager}, memoryManager{memoryManager},
          expressionMapper{}, catalog{catalog}, physicalOperatorID{0} {}

    std::unique_ptr<PhysicalPlan> mapLogicalPlanToPhysical(planner::LogicalPlan* logicalPlan,
        const binder::expression_vector& expressionsToCollect, common::StatementType statementType);

private:
    std::unique_ptr<PhysicalOperator> mapLogicalOperatorToPhysical(
        const std::shared_ptr<planner::LogicalOperator>& logicalOperator);

    std::unique_ptr<PhysicalOperator> mapLogicalScanNodeToPhysical(
        planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapLogicalIndexScanNodeToPhysical(
        planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapLogicalUnwindToPhysical(
        planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapLogicalExtendToPhysical(
        planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapLogicalFlattenToPhysical(
        planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapLogicalFilterToPhysical(
        planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapLogicalProjectionToPhysical(
        planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapLogicalScanNodePropertyToPhysical(
        planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapLogicalSemiMaskerToPhysical(
        planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapLogicalHashJoinToPhysical(
        planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapLogicalIntersectToPhysical(
        planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapLogicalCrossProductToPhysical(
        planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapLogicalMultiplicityReducerToPhysical(
        planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapLogicalSkipToPhysical(
        planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapLogicalLimitToPhysical(
        planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapLogicalAggregateToPhysical(
        planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapLogicalDistinctToPhysical(
        planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapLogicalOrderByToPhysical(
        planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapLogicalUnionAllToPhysical(
        planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapLogicalAccumulateToPhysical(
        planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapLogicalExpressionsScanToPhysical(
        planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapLogicalFTableScanToPhysical(
        planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapLogicalCreateNodeToPhysical(
        planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapLogicalCreateRelToPhysical(
        planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapLogicalSetNodePropertyToPhysical(
        planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapLogicalSetRelPropertyToPhysical(
        planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapLogicalDeleteNodeToPhysical(
        planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapLogicalDeleteRelToPhysical(
        planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapLogicalCreateNodeTableToPhysical(
        planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapLogicalCreateRelTableToPhysical(
        planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapLogicalCopyToPhysical(
        planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapLogicalDropTableToPhysical(
        planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapLogicalRenameTableToPhysical(
        planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapLogicalAddPropertyToPhysical(
        planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapLogicalDropPropertyToPhysical(
        planner::LogicalOperator* logicalOperator);
    std::unique_ptr<PhysicalOperator> mapLogicalRenamePropertyToPhysical(
        planner::LogicalOperator* logicalOperator);

    std::unique_ptr<ResultCollector> appendResultCollector(
        const binder::expression_vector& expressionsToCollect, const planner::Schema& schema,
        std::unique_ptr<PhysicalOperator> prevOperator);

    inline uint32_t getOperatorID() { return physicalOperatorID++; }

    std::unique_ptr<PhysicalOperator> createHashAggregate(
        std::vector<std::unique_ptr<function::AggregateFunction>> aggregateFunctions,
        std::vector<DataPos> inputAggVectorsPos, std::vector<DataPos> outputAggVectorsPos,
        std::vector<common::DataType> outputAggVectorsDataType,
        const binder::expression_vector& groupByExpressions,
        std::unique_ptr<PhysicalOperator> prevOperator, const planner::Schema& inSchema,
        const planner::Schema& outSchema, const std::string& paramsString);

    void appendGroupByExpressions(const binder::expression_vector& groupByExpressions,
        std::vector<DataPos>& inputGroupByHashKeyVectorsPos,
        std::vector<DataPos>& outputGroupByKeyVectorsPos,
        std::vector<common::DataType>& outputGroupByKeyVectorsDataTypes,
        const planner::Schema& inSchema, const planner::Schema& outSchema,
        std::vector<bool>& isInputGroupByHashKeyVectorFlat);

    BuildDataInfo generateBuildDataInfo(const planner::Schema& buildSideSchema,
        const binder::expression_vector& keys, const binder::expression_vector& payloads);

    void mapAccHashJoin(PhysicalOperator* probe);

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
