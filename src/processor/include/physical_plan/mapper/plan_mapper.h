#pragma once

#include "src/planner/include/logical_plan/logical_plan.h"
#include "src/processor/include/physical_plan/mapper/expression_mapper.h"
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
        : graph{graph}, outerPhysicalOperatorsInfo{nullptr}, physicalOperatorID{0},
          physicalIDToLogicalOperatorMap{}, expressionMapper{} {}

    unique_ptr<PhysicalPlan> mapToPhysical(
        unique_ptr<LogicalPlan> logicalPlan, ExecutionContext& executionContext);

    // Returns current physicalOperatorsInfo whoever calls enterSubquery is responsible to save the
    // return physicalOperatorsInfo and pass it back when calling exitSubquery()
    const PhysicalOperatorsInfo* enterSubquery(
        const PhysicalOperatorsInfo* newPhysicalOperatorsInfo);
    void exitSubquery(const PhysicalOperatorsInfo* prevPhysicalOperatorsInfo);

private:
    unique_ptr<PhysicalOperator> mapLogicalOperatorToPhysical(
        const shared_ptr<LogicalOperator>& logicalOperator, PhysicalOperatorsInfo& info,
        ExecutionContext& executionContext);

    unique_ptr<PhysicalOperator> mapLogicalScanNodeIDToPhysical(
        LogicalOperator* logicalOperator, PhysicalOperatorsInfo& info, ExecutionContext& context);
    unique_ptr<PhysicalOperator> mapLogicalSelectScanToPhysical(
        LogicalOperator* logicalOperator, PhysicalOperatorsInfo& info, ExecutionContext& context);
    unique_ptr<PhysicalOperator> mapLogicalExtendToPhysical(
        LogicalOperator* logicalOperator, PhysicalOperatorsInfo& info, ExecutionContext& context);
    unique_ptr<PhysicalOperator> mapLogicalFlattenToPhysical(
        LogicalOperator* logicalOperator, PhysicalOperatorsInfo& info, ExecutionContext& context);
    unique_ptr<PhysicalOperator> mapLogicalFilterToPhysical(
        LogicalOperator* logicalOperator, PhysicalOperatorsInfo& info, ExecutionContext& context);
    unique_ptr<PhysicalOperator> mapLogicalIntersectToPhysical(
        LogicalOperator* logicalOperator, PhysicalOperatorsInfo& info, ExecutionContext& context);
    unique_ptr<PhysicalOperator> mapLogicalProjectionToPhysical(
        LogicalOperator* logicalOperator, PhysicalOperatorsInfo& info, ExecutionContext& context);
    unique_ptr<PhysicalOperator> mapLogicalScanNodePropertyToPhysical(
        LogicalOperator* logicalOperator, PhysicalOperatorsInfo& info, ExecutionContext& context);
    unique_ptr<PhysicalOperator> mapLogicalScanRelPropertyToPhysical(
        LogicalOperator* logicalOperator, PhysicalOperatorsInfo& info, ExecutionContext& context);
    unique_ptr<PhysicalOperator> mapLogicalHashJoinToPhysical(
        LogicalOperator* logicalOperator, PhysicalOperatorsInfo& info, ExecutionContext& context);
    unique_ptr<PhysicalOperator> mapLogicalMultiplicityReducerToPhysical(
        LogicalOperator* logicalOperator, PhysicalOperatorsInfo& info, ExecutionContext& context);
    unique_ptr<PhysicalOperator> mapLogicalSkipToPhysical(
        LogicalOperator* logicalOperator, PhysicalOperatorsInfo& info, ExecutionContext& context);
    unique_ptr<PhysicalOperator> mapLogicalLimitToPhysical(
        LogicalOperator* logicalOperator, PhysicalOperatorsInfo& info, ExecutionContext& context);
    unique_ptr<PhysicalOperator> mapLogicalAggregateToPhysical(
        LogicalOperator* logicalOperator, PhysicalOperatorsInfo& info, ExecutionContext& context);
    unique_ptr<PhysicalOperator> mapLogicalExistsToPhysical(
        LogicalOperator* logicalOperator, PhysicalOperatorsInfo& info, ExecutionContext& context);

public:
    const Graph& graph;

    const PhysicalOperatorsInfo* outerPhysicalOperatorsInfo;

    uint32_t physicalOperatorID;
    // used for EXPLAIN & PROFILE which require print physical operator and logical information.
    // e.g. SCAN_NODE_ID (a), SCAN_NODE_ID is physical operator name and "a" is logical information.
    unordered_map<uint32_t, shared_ptr<LogicalOperator>> physicalIDToLogicalOperatorMap;

    ExpressionMapper expressionMapper;
};

} // namespace processor
} // namespace graphflow
