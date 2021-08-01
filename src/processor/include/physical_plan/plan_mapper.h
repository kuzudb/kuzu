#pragma once

#include "src/binder/include/expression/expression.h"
#include "src/planner/include/logical_plan/logical_plan.h"
#include "src/processor/include/physical_plan/operator/physical_operator_info.h"
#include "src/processor/include/physical_plan/physical_plan.h"
#include "src/storage/include/graph.h"

using namespace graphflow::binder;
using namespace graphflow::planner;

namespace graphflow {
namespace processor {

class PlanMapper {

public:
    explicit PlanMapper(const Graph& graph) : graph{graph}, physicalOperatorID{0u} {};

    unique_ptr<PhysicalPlan> mapToPhysical(
        unique_ptr<LogicalPlan> logicalPlan, ExecutionContext& executionContext);

private:
    unique_ptr<PhysicalOperator> mapLogicalOperatorToPhysical(
        const shared_ptr<LogicalOperator>& logicalOperator, PhysicalOperatorsInfo& info,
        ExecutionContext& executionContext);

    unique_ptr<PhysicalOperator> mapLogicalScanNodeIDToPhysical(
        LogicalOperator* logicalOperator, PhysicalOperatorsInfo& info, ExecutionContext& context);
    unique_ptr<PhysicalOperator> mapLogicalExtendToPhysical(
        LogicalOperator* logicalOperator, PhysicalOperatorsInfo& info, ExecutionContext& context);
    unique_ptr<PhysicalOperator> mapLogicalFlattenToPhysical(
        LogicalOperator* logicalOperator, PhysicalOperatorsInfo& info, ExecutionContext& context);
    unique_ptr<PhysicalOperator> mapLogicalFilterToPhysical(
        LogicalOperator* logicalOperator, PhysicalOperatorsInfo& info, ExecutionContext& context);
    unique_ptr<PhysicalOperator> mapLogicalProjectionToPhysical(
        LogicalOperator* logicalOperator, PhysicalOperatorsInfo& info, ExecutionContext& context);
    unique_ptr<PhysicalOperator> mapLogicalScanNodePropertyToPhysical(
        LogicalOperator* logicalOperator, PhysicalOperatorsInfo& info, ExecutionContext& context);
    unique_ptr<PhysicalOperator> mapLogicalScanRelPropertyToPhysical(
        LogicalOperator* logicalOperator, PhysicalOperatorsInfo& info, ExecutionContext& context);
    unique_ptr<PhysicalOperator> mapLogicalHashJoinToPhysical(
        LogicalOperator* logicalOperator, PhysicalOperatorsInfo& info, ExecutionContext& context);
    unique_ptr<PhysicalOperator> mapLogicalLoadCSVToPhysical(
        LogicalOperator* logicalOperator, PhysicalOperatorsInfo& info, ExecutionContext& context);
    unique_ptr<PhysicalOperator> mapLogicalLimitToPhysical(
        LogicalOperator* logicalOperator, PhysicalOperatorsInfo& info, ExecutionContext& context);
    unique_ptr<PhysicalOperator> mapLogicalSkipToPhysical(
        LogicalOperator* logicalOperator, PhysicalOperatorsInfo& info, ExecutionContext& context);

public:
    const Graph& graph;
    uint32_t physicalOperatorID;
    // used for EXPLAIN & PROFILE which require print physical operator and logical information.
    // e.g. SCAN_NODE_ID (a), SCAN_NODE_ID is physical operator name and "a" is logical information.
    unordered_map<uint32_t, shared_ptr<LogicalOperator>> physicalIDToLogicalOperatorMap;
};

} // namespace processor
} // namespace graphflow
