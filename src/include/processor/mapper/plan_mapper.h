#pragma once

#include "binder/expression/node_expression.h"
#include "common/statement_type.h"
#include "planner/logical_plan/logical_plan.h"
#include "processor/mapper/expression_mapper.h"
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

    unique_ptr<PhysicalPlan> mapLogicalPlanToPhysical(LogicalPlan* logicalPlan,
        const expression_vector& expressionsToCollect, common::StatementType statementType);

private:
    unique_ptr<PhysicalOperator> mapLogicalOperatorToPhysical(
        const shared_ptr<LogicalOperator>& logicalOperator);

    unique_ptr<PhysicalOperator> mapLogicalScanNodeToPhysical(LogicalOperator* logicalOperator);
    unique_ptr<PhysicalOperator> mapLogicalIndexScanNodeToPhysical(
        LogicalOperator* logicalOperator);
    unique_ptr<PhysicalOperator> mapLogicalUnwindToPhysical(LogicalOperator* logicalOperator);
    unique_ptr<PhysicalOperator> mapLogicalExtendToPhysical(LogicalOperator* logicalOperator);
    unique_ptr<PhysicalOperator> mapLogicalFlattenToPhysical(LogicalOperator* logicalOperator);
    unique_ptr<PhysicalOperator> mapLogicalFilterToPhysical(LogicalOperator* logicalOperator);
    unique_ptr<PhysicalOperator> mapLogicalProjectionToPhysical(LogicalOperator* logicalOperator);
    unique_ptr<PhysicalOperator> mapLogicalScanNodePropertyToPhysical(
        LogicalOperator* logicalOperator);
    unique_ptr<PhysicalOperator> mapLogicalSemiMaskerToPhysical(LogicalOperator* logicalOperator);
    unique_ptr<PhysicalOperator> mapLogicalHashJoinToPhysical(LogicalOperator* logicalOperator);
    unique_ptr<PhysicalOperator> mapLogicalIntersectToPhysical(LogicalOperator* logicalOperator);
    unique_ptr<PhysicalOperator> mapLogicalCrossProductToPhysical(LogicalOperator* logicalOperator);
    unique_ptr<PhysicalOperator> mapLogicalMultiplicityReducerToPhysical(
        LogicalOperator* logicalOperator);
    unique_ptr<PhysicalOperator> mapLogicalSkipToPhysical(LogicalOperator* logicalOperator);
    unique_ptr<PhysicalOperator> mapLogicalLimitToPhysical(LogicalOperator* logicalOperator);
    unique_ptr<PhysicalOperator> mapLogicalAggregateToPhysical(LogicalOperator* logicalOperator);
    unique_ptr<PhysicalOperator> mapLogicalDistinctToPhysical(LogicalOperator* logicalOperator);
    unique_ptr<PhysicalOperator> mapLogicalOrderByToPhysical(LogicalOperator* logicalOperator);
    unique_ptr<PhysicalOperator> mapLogicalUnionAllToPhysical(LogicalOperator* logicalOperator);
    unique_ptr<PhysicalOperator> mapLogicalAccumulateToPhysical(LogicalOperator* logicalOperator);
    unique_ptr<PhysicalOperator> mapLogicalExpressionsScanToPhysical(
        LogicalOperator* logicalOperator);
    unique_ptr<PhysicalOperator> mapLogicalFTableScanToPhysical(LogicalOperator* logicalOperator);
    unique_ptr<PhysicalOperator> mapLogicalCreateNodeToPhysical(LogicalOperator* logicalOperator);
    unique_ptr<PhysicalOperator> mapLogicalCreateRelToPhysical(LogicalOperator* logicalOperator);
    unique_ptr<PhysicalOperator> mapLogicalSetNodePropertyToPhysical(
        LogicalOperator* logicalOperator);
    unique_ptr<PhysicalOperator> mapLogicalSetRelPropertyToPhysical(
        LogicalOperator* logicalOperator);
    unique_ptr<PhysicalOperator> mapLogicalDeleteNodeToPhysical(LogicalOperator* logicalOperator);
    unique_ptr<PhysicalOperator> mapLogicalDeleteRelToPhysical(LogicalOperator* logicalOperator);
    unique_ptr<PhysicalOperator> mapLogicalCreateNodeTableToPhysical(
        LogicalOperator* logicalOperator);
    unique_ptr<PhysicalOperator> mapLogicalCreateRelTableToPhysical(
        LogicalOperator* logicalOperator);
    unique_ptr<PhysicalOperator> mapLogicalCopyToPhysical(LogicalOperator* logicalOperator);
    unique_ptr<PhysicalOperator> mapLogicalDropTableToPhysical(LogicalOperator* logicalOperator);
    unique_ptr<PhysicalOperator> mapLogicalDropPropertyToPhysical(LogicalOperator* logicalOperator);
    unique_ptr<PhysicalOperator> mapLogicalAddPropertyToPhysical(LogicalOperator* logicalOperator);

    unique_ptr<ResultCollector> appendResultCollector(const expression_vector& expressionsToCollect,
        const Schema& schema, unique_ptr<PhysicalOperator> prevOperator);

    inline uint32_t getOperatorID() { return physicalOperatorID++; }

    unique_ptr<PhysicalOperator> createHashAggregate(
        vector<unique_ptr<AggregateFunction>> aggregateFunctions,
        vector<DataPos> inputAggVectorsPos, vector<DataPos> outputAggVectorsPos,
        vector<DataType> outputAggVectorsDataType, const expression_vector& groupByExpressions,
        unique_ptr<PhysicalOperator> prevOperator, const Schema& inSchema, const Schema& outSchema,
        const string& paramsString);

    void appendGroupByExpressions(const expression_vector& groupByExpressions,
        vector<DataPos>& inputGroupByHashKeyVectorsPos, vector<DataPos>& outputGroupByKeyVectorsPos,
        vector<DataType>& outputGroupByKeyVectorsDataTypes, const Schema& inSchema,
        const Schema& outSchema, vector<bool>& isInputGroupByHashKeyVectorFlat);

    static BuildDataInfo generateBuildDataInfo(const Schema& buildSideSchema,
        const expression_vector& keys, const expression_vector& payloads);

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
