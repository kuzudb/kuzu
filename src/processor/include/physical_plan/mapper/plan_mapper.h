#pragma once

#include "src/planner/logical_plan/include/logical_plan.h"
#include "src/processor/include/physical_plan/mapper/expression_mapper.h"
#include "src/processor/include/physical_plan/mapper/mapper_context.h"
#include "src/processor/include/physical_plan/operator/result_collector.h"
#include "src/processor/include/physical_plan/physical_plan.h"
#include "src/storage/include/storage_manager.h"
#include "src/storage/store/include/nodes_statistics_and_deleted_ids.h"

using namespace graphflow::storage;
using namespace graphflow::planner;

namespace graphflow {
namespace processor {

class PlanMapper {
public:
    // Create plan mapper with default mapper context.
    PlanMapper(StorageManager& storageManager, MemoryManager* memoryManager, Catalog* catalog)
        : storageManager{storageManager}, memoryManager{memoryManager}, outerMapperContext{nullptr},
          expressionMapper{}, catalog{catalog}, physicalOperatorID{0} {}

    unique_ptr<PhysicalPlan> mapLogicalPlanToPhysical(unique_ptr<LogicalPlan> logicalPlan);

private:
    // Returns current physicalOperatorsInfo whoever calls enterSubquery is responsible to save the
    // return physicalOperatorsInfo and pass it back when calling exitSubquery()
    const MapperContext* enterSubquery(const MapperContext* newMapperContext);
    void exitSubquery(const MapperContext* prevMapperContext);

    unique_ptr<PhysicalOperator> mapLogicalOperatorToPhysical(
        const shared_ptr<LogicalOperator>& logicalOperator, MapperContext& mapperContext);

    unique_ptr<PhysicalOperator> mapLogicalScanNodeIDToPhysical(
        LogicalOperator* logicalOperator, MapperContext& mapperContext);
    unique_ptr<PhysicalOperator> mapLogicalResultScanToPhysical(
        LogicalOperator* logicalOperator, MapperContext& mapperContext);
    unique_ptr<PhysicalOperator> mapLogicalExtendToPhysical(
        LogicalOperator* logicalOperator, MapperContext& mapperContext);
    unique_ptr<PhysicalOperator> mapLogicalFlattenToPhysical(
        LogicalOperator* logicalOperator, MapperContext& mapperContext);
    unique_ptr<PhysicalOperator> mapLogicalFilterToPhysical(
        LogicalOperator* logicalOperator, MapperContext& mapperContext);
    unique_ptr<PhysicalOperator> mapLogicalIntersectToPhysical(
        LogicalOperator* logicalOperator, MapperContext& mapperContext);
    unique_ptr<PhysicalOperator> mapLogicalProjectionToPhysical(
        LogicalOperator* logicalOperator, MapperContext& mapperContext);
    unique_ptr<PhysicalOperator> mapLogicalScanNodePropertyToPhysical(
        LogicalOperator* logicalOperator, MapperContext& mapperContext);
    unique_ptr<PhysicalOperator> mapLogicalScanRelPropertyToPhysical(
        LogicalOperator* logicalOperator, MapperContext& mapperContext);
    unique_ptr<PhysicalOperator> mapLogicalHashJoinToPhysical(
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
    unique_ptr<PhysicalOperator> mapLogicalExistsToPhysical(
        LogicalOperator* logicalOperator, MapperContext& mapperContext);
    unique_ptr<PhysicalOperator> mapLogicalOrderByToPhysical(
        LogicalOperator* logicalOperator, MapperContext& mapperContext);
    unique_ptr<PhysicalOperator> mapLogicalUnionAllToPhysical(
        LogicalOperator* logicalOperator, MapperContext& mapperContext);
    unique_ptr<PhysicalOperator> mapLogicalAccumulateToPhysical(
        LogicalOperator* logicalOperator, MapperContext& mapperContext);
    unique_ptr<PhysicalOperator> mapLogicalTableScanToPhysical(
        LogicalOperator* logicalOperator, MapperContext& mapperContext);
    unique_ptr<PhysicalOperator> mapLogicalCreateToPhysical(
        LogicalOperator* logicalOperator, MapperContext& mapperContext);
    unique_ptr<PhysicalOperator> mapLogicalSetToPhysical(
        LogicalOperator* logicalOperator, MapperContext& mapperContext);
    unique_ptr<PhysicalOperator> mapLogicalDeleteToPhysical(
        LogicalOperator* logicalOperator, MapperContext& mapperContext);
    unique_ptr<PhysicalOperator> mapLogicalCreateNodeTableToPhysical(
        LogicalOperator* logicalOperator, MapperContext& mapperContext);
    unique_ptr<PhysicalOperator> mapLogicalCreateRelTableToPhysical(
        LogicalOperator* logicalOperator, MapperContext& mapperContext);
    unique_ptr<PhysicalOperator> mapLogicalCopyCSVToPhysical(
        LogicalOperator* logicalOperator, MapperContext& mapperContext);
    unique_ptr<PhysicalOperator> mapLogicalDropTableToPhysical(
        LogicalOperator* logicalOperator, MapperContext& mapperContext);

    unique_ptr<ResultCollector> appendResultCollector(expression_vector expressionsToCollect,
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

public:
    StorageManager& storageManager;
    MemoryManager* memoryManager;
    const MapperContext* outerMapperContext;
    ExpressionMapper expressionMapper;
    Catalog* catalog;

private:
    uint32_t physicalOperatorID;
};

} // namespace processor
} // namespace graphflow
