#pragma once

#include "src/planner/include/logical_plan/logical_plan.h"
#include "src/processor/include/physical_plan/mapper/expression_mapper.h"
#include "src/processor/include/physical_plan/mapper/mapper_context.h"
#include "src/processor/include/physical_plan/physical_plan.h"
#include "src/storage/include/graph.h"

using namespace graphflow::storage;
using namespace graphflow::planner;

namespace graphflow {
namespace processor {

class PlanMapper {

public:
    // Create plan mapper with default mapper context.
    explicit PlanMapper(const Graph& graph)
        : graph{graph}, outerMapperContext{nullptr}, expressionMapper{} {}

    unique_ptr<PhysicalPlan> mapLogicalPlanToPhysical(
        unique_ptr<LogicalPlan> logicalPlan, ExecutionContext& executionContext);

private:
    // Returns current physicalOperatorsInfo whoever calls enterSubquery is responsible to save the
    // return physicalOperatorsInfo and pass it back when calling exitSubquery()
    const MapperContext* enterSubquery(const MapperContext* newMapperContext);
    void exitSubquery(const MapperContext* prevMapperContext);

    unique_ptr<PhysicalOperator> mapLogicalOperatorToPhysical(
        const shared_ptr<LogicalOperator>& logicalOperator, MapperContext& mapperContext,
        ExecutionContext& executionContext);

    unique_ptr<PhysicalOperator> mapLogicalScanNodeIDToPhysical(LogicalOperator* logicalOperator,
        MapperContext& mapperContext, ExecutionContext& executionContext);
    unique_ptr<PhysicalOperator> mapLogicalResultScanToPhysical(LogicalOperator* logicalOperator,
        MapperContext& mapperContext, ExecutionContext& executionContext);
    unique_ptr<PhysicalOperator> mapLogicalExtendToPhysical(LogicalOperator* logicalOperator,
        MapperContext& mapperContext, ExecutionContext& executionContext);
    unique_ptr<PhysicalOperator> mapLogicalFlattenToPhysical(LogicalOperator* logicalOperator,
        MapperContext& mapperContext, ExecutionContext& executionContext);
    unique_ptr<PhysicalOperator> mapLogicalFilterToPhysical(LogicalOperator* logicalOperator,
        MapperContext& mapperContext, ExecutionContext& executionContext);
    unique_ptr<PhysicalOperator> mapLogicalIntersectToPhysical(LogicalOperator* logicalOperator,
        MapperContext& mapperContext, ExecutionContext& executionContext);
    unique_ptr<PhysicalOperator> mapLogicalProjectionToPhysical(LogicalOperator* logicalOperator,
        MapperContext& mapperContext, ExecutionContext& executionContext);
    unique_ptr<PhysicalOperator> mapLogicalScanNodePropertyToPhysical(
        LogicalOperator* logicalOperator, MapperContext& mapperContext,
        ExecutionContext& executionContext);
    unique_ptr<PhysicalOperator> mapLogicalScanRelPropertyToPhysical(
        LogicalOperator* logicalOperator, MapperContext& mapperContext,
        ExecutionContext& executionContext);
    unique_ptr<PhysicalOperator> mapLogicalHashJoinToPhysical(LogicalOperator* logicalOperator,
        MapperContext& mapperContext, ExecutionContext& executionContext);
    unique_ptr<PhysicalOperator> mapLogicalMultiplicityReducerToPhysical(
        LogicalOperator* logicalOperator, MapperContext& mapperContext,
        ExecutionContext& executionContext);
    unique_ptr<PhysicalOperator> mapLogicalSkipToPhysical(LogicalOperator* logicalOperator,
        MapperContext& mapperContext, ExecutionContext& executionContext);
    unique_ptr<PhysicalOperator> mapLogicalLimitToPhysical(LogicalOperator* logicalOperator,
        MapperContext& mapperContext, ExecutionContext& executionContext);
    unique_ptr<PhysicalOperator> mapLogicalAggregateToPhysical(LogicalOperator* logicalOperator,
        MapperContext& mapperContext, ExecutionContext& executionContext);
    unique_ptr<PhysicalOperator> mapLogicalExistsToPhysical(LogicalOperator* logicalOperator,
        MapperContext& mapperContext, ExecutionContext& executionContext);
    unique_ptr<PhysicalOperator> mapLogicalLeftNestedLoopJoinToPhysical(
        LogicalOperator* logicalOperator, MapperContext& mapperContext,
        ExecutionContext& executionContext);
    unique_ptr<PhysicalOperator> mapLogicalOrderByToPhysical(LogicalOperator* logicalOperator,
        MapperContext& mapperContext, ExecutionContext& executionContext);

public:
    const Graph& graph;
    const MapperContext* outerMapperContext;
    ExpressionMapper expressionMapper;

    // used for EXPLAIN & PROFILE which require print physical operator and logical information.
    // e.g. SCAN_NODE_ID (a), SCAN_NODE_ID is physical operator name and "a" is logical information.
    unordered_map<uint32_t, shared_ptr<LogicalOperator>> physicalIDToLogicalOperatorMap;
};

} // namespace processor
} // namespace graphflow
