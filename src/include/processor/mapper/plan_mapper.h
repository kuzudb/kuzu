#pragma once

#include "binder/expression/node_expression.h"
#include "planner/logical_plan/logical_plan.h"
#include "processor/mapper/expression_mapper.h"
#include "processor/mapper/mapper_context.h"
#include "processor/operator/result_collector.h"
#include "processor/physical_plan.h"
#include "storage/storage_manager.h"
#include "storage/store/nodes_statistics_and_deleted_ids.h"

using namespace kuzu::storage;
using namespace kuzu::planner;

namespace kuzu {
namespace processor {

struct BuildDataInfo;

class PlanMapper {
public:
    // Create plan mapper with default mapper context.
    PlanMapper(StorageManager& storageManager, MemoryManager* memoryManager, Catalog* catalog)
        : storageManager{storageManager}, memoryManager{memoryManager},
          expressionMapper{}, catalog{catalog}, physicalOperatorID{0} {}

    unique_ptr<PhysicalPlan> mapLogicalPlanToPhysical(LogicalPlan* logicalPlan);

private:
    unique_ptr<PhysicalOperator> mapLogicalOperatorToPhysical(
        const shared_ptr<LogicalOperator>& logicalOperator, MapperContext& mapperContext);

    unique_ptr<PhysicalOperator> mapLogicalScanNodeToPhysical(
        LogicalOperator* logicalOperator, MapperContext& mapperContext);
    unique_ptr<PhysicalOperator> mapLogicalIndexScanNodeToPhysical(
        LogicalOperator* logicalOperator, MapperContext& mapperContext);
    unique_ptr<PhysicalOperator> mapLogicalUnwindToPhysical(
        LogicalOperator* logicalOperator, MapperContext& mapperContext);
    unique_ptr<PhysicalOperator> mapLogicalExtendToPhysical(
        LogicalOperator* logicalOperator, MapperContext& mapperContext);
    unique_ptr<PhysicalOperator> mapLogicalFlattenToPhysical(
        LogicalOperator* logicalOperator, MapperContext& mapperContext);
    unique_ptr<PhysicalOperator> mapLogicalFilterToPhysical(
        LogicalOperator* logicalOperator, MapperContext& mapperContext);
    unique_ptr<PhysicalOperator> mapLogicalProjectionToPhysical(
        LogicalOperator* logicalOperator, MapperContext& mapperContext);
    unique_ptr<PhysicalOperator> mapLogicalScanNodePropertyToPhysical(
        LogicalOperator* logicalOperator, MapperContext& mapperContext);
    unique_ptr<PhysicalOperator> mapLogicalScanRelPropertyToPhysical(
        LogicalOperator* logicalOperator, MapperContext& mapperContext);
    unique_ptr<PhysicalOperator> mapLogicalSemiMaskerToPhysical(
        LogicalOperator* logicalOperator, MapperContext& mapperContext);
    unique_ptr<PhysicalOperator> mapLogicalHashJoinToPhysical(
        LogicalOperator* logicalOperator, MapperContext& mapperContext);
    unique_ptr<PhysicalOperator> mapLogicalIntersectToPhysical(
        LogicalOperator* logicalOperator, MapperContext& mapperContext);
    unique_ptr<PhysicalOperator> mapLogicalCrossProductToPhysical(
        LogicalOperator* logicalOperator, MapperContext& mapperContext);
    unique_ptr<PhysicalOperator> mapLogicalMultiplicityReducerToPhysical(
        LogicalOperator* logicalOperator, MapperContext& mapperContext);
    unique_ptr<PhysicalOperator> mapLogicalSkipToPhysical(
        LogicalOperator* logicalOperator, MapperContext& mapperContext);
    unique_ptr<PhysicalOperator> mapLogicalLimitToPhysical(
        LogicalOperator* logicalOperator, MapperContext& mapperContext);
    unique_ptr<PhysicalOperator> mapLogicalAggregateToPhysical(
        LogicalOperator* logicalOperator, MapperContext& mapperContext);
    unique_ptr<PhysicalOperator> mapLogicalDistinctToPhysical(
        LogicalOperator* logicalOperator, MapperContext& mapperContext);
    unique_ptr<PhysicalOperator> mapLogicalOrderByToPhysical(
        LogicalOperator* logicalOperator, MapperContext& mapperContext);
    unique_ptr<PhysicalOperator> mapLogicalUnionAllToPhysical(
        LogicalOperator* logicalOperator, MapperContext& mapperContext);
    unique_ptr<PhysicalOperator> mapLogicalAccumulateToPhysical(
        LogicalOperator* logicalOperator, MapperContext& mapperContext);
    unique_ptr<PhysicalOperator> mapLogicalExpressionsScanToPhysical(
        LogicalOperator* logicalOperator, MapperContext& mapperContext);
    unique_ptr<PhysicalOperator> mapLogicalFTableScanToPhysical(
        LogicalOperator* logicalOperator, MapperContext& mapperContext);
    unique_ptr<PhysicalOperator> mapLogicalCreateNodeToPhysical(
        LogicalOperator* logicalOperator, MapperContext& mapperContext);
    unique_ptr<PhysicalOperator> mapLogicalCreateRelToPhysical(
        LogicalOperator* logicalOperator, MapperContext& mapperContext);
    unique_ptr<PhysicalOperator> mapLogicalSetToPhysical(
        LogicalOperator* logicalOperator, MapperContext& mapperContext);
    unique_ptr<PhysicalOperator> mapLogicalDeleteNodeToPhysical(
        LogicalOperator* logicalOperator, MapperContext& mapperContext);
    unique_ptr<PhysicalOperator> mapLogicalDeleteRelToPhysical(
        LogicalOperator* logicalOperator, MapperContext& mapperContext);
    unique_ptr<PhysicalOperator> mapLogicalCreateNodeTableToPhysical(
        LogicalOperator* logicalOperator, MapperContext& mapperContext);
    unique_ptr<PhysicalOperator> mapLogicalCreateRelTableToPhysical(
        LogicalOperator* logicalOperator, MapperContext& mapperContext);
    unique_ptr<PhysicalOperator> mapLogicalCopyCSVToPhysical(
        LogicalOperator* logicalOperator, MapperContext& mapperContext);
    unique_ptr<PhysicalOperator> mapLogicalDropTableToPhysical(
        LogicalOperator* logicalOperator, MapperContext& mapperContext);

    unique_ptr<ResultCollector> appendResultCollector(const expression_vector& expressionsToCollect,
        const Schema& schema, unique_ptr<PhysicalOperator> prevOperator,
        MapperContext& mapperContext);

    inline uint32_t getOperatorID() { return physicalOperatorID++; }

    unique_ptr<PhysicalOperator> createHashAggregate(
        vector<unique_ptr<AggregateFunction>> aggregateFunctions,
        vector<DataPos> inputAggVectorsPos, vector<DataPos> outputAggVectorsPos,
        vector<DataType> outputAggVectorsDataType, const expression_vector& groupByExpressions,
        Schema* schema, unique_ptr<PhysicalOperator> prevOperator,
        MapperContext& mapperContextBeforeAggregate, MapperContext& mapperContext,
        const string& paramsString);

    void appendGroupByExpressions(const expression_vector& groupByExpressions,
        vector<DataPos>& inputGroupByHashKeyVectorsPos, vector<DataPos>& outputGroupByKeyVectorsPos,
        vector<DataType>& outputGroupByKeyVectorsDataTypes,
        MapperContext& mapperContextBeforeAggregate, MapperContext& mapperContext, Schema* schema,
        vector<bool>& isInputGroupByHashKeyVectorFlat);

    static BuildDataInfo generateBuildDataInfo(MapperContext& mapperContext,
        Schema* buildSideSchema, const vector<shared_ptr<NodeExpression>>& keys,
        const expression_vector& payloads);

public:
    StorageManager& storageManager;
    MemoryManager* memoryManager;
    ExpressionMapper expressionMapper;
    Catalog* catalog;

private:
    uint32_t physicalOperatorID;
};

} // namespace processor
} // namespace kuzu
