#include "processor/mapper/plan_mapper.h"

#include <set>

#include "processor/mapper/expression_mapper.h"
#include "processor/operator/result_collector.h"

using namespace kuzu::common;
using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalPlan> PlanMapper::mapLogicalPlanToPhysical(
    LogicalPlan* logicalPlan, const binder::expression_vector& expressionsToCollect) {
    auto lastOperator = mapLogicalOperatorToPhysical(logicalPlan->getLastOperator());
    // We have a special code path for executing copy rel and copy npy, so we don't need to append
    // the resultCollector.
    if (lastOperator->getOperatorType() != PhysicalOperatorType::COPY_REL &&
        lastOperator->getOperatorType() != PhysicalOperatorType::COPY_NPY) {
        lastOperator = appendResultCollector(
            expressionsToCollect, logicalPlan->getSchema(), std::move(lastOperator));
    }
    return make_unique<PhysicalPlan>(std::move(lastOperator));
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapLogicalOperatorToPhysical(
    const std::shared_ptr<LogicalOperator>& logicalOperator) {
    std::unique_ptr<PhysicalOperator> physicalOperator;
    auto operatorType = logicalOperator->getOperatorType();
    switch (operatorType) {
    case LogicalOperatorType::SCAN_FRONTIER: {
        physicalOperator = mapLogicalScanFrontierToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::SCAN_NODE: {
        physicalOperator = mapLogicalScanNodeToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::INDEX_SCAN_NODE: {
        physicalOperator = mapLogicalIndexScanNodeToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::UNWIND: {
        physicalOperator = mapLogicalUnwindToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::EXTEND: {
        physicalOperator = mapLogicalExtendToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::RECURSIVE_EXTEND: {
        physicalOperator = mapLogicalRecursiveExtendToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::PATH_PROPERTY_PROBE: {
        physicalOperator = mapLogicalPathPropertyProbeToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::FLATTEN: {
        physicalOperator = mapLogicalFlattenToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::FILTER: {
        physicalOperator = mapLogicalFilterToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::PROJECTION: {
        physicalOperator = mapLogicalProjectionToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::SEMI_MASKER: {
        physicalOperator = mapLogicalSemiMaskerToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::HASH_JOIN: {
        physicalOperator = mapLogicalHashJoinToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::INTERSECT: {
        physicalOperator = mapLogicalIntersectToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::CROSS_PRODUCT: {
        physicalOperator = mapLogicalCrossProductToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::SCAN_NODE_PROPERTY: {
        physicalOperator = mapLogicalScanNodePropertyToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::MULTIPLICITY_REDUCER: {
        physicalOperator = mapLogicalMultiplicityReducerToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::SKIP: {
        physicalOperator = mapLogicalSkipToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::LIMIT: {
        physicalOperator = mapLogicalLimitToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::AGGREGATE: {
        physicalOperator = mapLogicalAggregateToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::DISTINCT: {
        physicalOperator = mapLogicalDistinctToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::ORDER_BY: {
        physicalOperator = mapLogicalOrderByToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::UNION_ALL: {
        physicalOperator = mapLogicalUnionAllToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::ACCUMULATE: {
        physicalOperator = mapLogicalAccumulateToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::EXPRESSIONS_SCAN: {
        physicalOperator = mapLogicalExpressionsScanToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::FTABLE_SCAN: {
        physicalOperator = mapLogicalFTableScanToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::CREATE_NODE: {
        physicalOperator = mapLogicalCreateNodeToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::CREATE_REL: {
        physicalOperator = mapLogicalCreateRelToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::SET_NODE_PROPERTY: {
        physicalOperator = mapLogicalSetNodePropertyToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::SET_REL_PROPERTY: {
        physicalOperator = mapLogicalSetRelPropertyToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::DELETE_NODE: {
        physicalOperator = mapLogicalDeleteNodeToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::DELETE_REL: {
        physicalOperator = mapLogicalDeleteRelToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::CREATE_NODE_TABLE: {
        physicalOperator = mapLogicalCreateNodeTableToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::CREATE_REL_TABLE: {
        physicalOperator = mapLogicalCreateRelTableToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::COPY: {
        physicalOperator = mapLogicalCopyToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::DROP_TABLE: {
        physicalOperator = mapLogicalDropTableToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::RENAME_TABLE: {
        physicalOperator = mapLogicalRenameTableToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::ADD_PROPERTY: {
        physicalOperator = mapLogicalAddPropertyToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::DROP_PROPERTY: {
        physicalOperator = mapLogicalDropPropertyToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::RENAME_PROPERTY: {
        physicalOperator = mapLogicalRenamePropertyToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::CALL: {
        physicalOperator = mapLogicalCallToPhysical(logicalOperator.get());
    } break;
    default:
        throw common::NotImplementedException("PlanMapper::mapLogicalOperatorToPhysical()");
    }
    logicalOpToPhysicalOpMap.insert({logicalOperator.get(), physicalOperator.get()});
    return physicalOperator;
}

std::unique_ptr<ResultCollector> PlanMapper::appendResultCollector(
    const binder::expression_vector& expressionsToCollect, Schema* schema,
    std::unique_ptr<PhysicalOperator> prevOperator) {
    bool hasUnFlatColumn = false;
    std::vector<DataPos> payloadsPos;
    auto tableSchema = std::make_unique<FactorizedTableSchema>();
    for (auto& expression : expressionsToCollect) {
        auto dataPos = DataPos(schema->getExpressionPos(*expression));
        std::unique_ptr<ColumnSchema> columnSchema;
        if (schema->getGroup(dataPos.dataChunkPos)->isFlat()) {
            columnSchema = std::make_unique<ColumnSchema>(false /* isUnFlat */,
                dataPos.dataChunkPos, LogicalTypeUtils::getRowLayoutSize(expression->dataType));
        } else {
            columnSchema = std::make_unique<ColumnSchema>(
                true /* isUnFlat */, dataPos.dataChunkPos, (uint32_t)sizeof(overflow_value_t));
            hasUnFlatColumn = true;
        }
        tableSchema->appendColumn(std::move(columnSchema));
        payloadsPos.push_back(dataPos);
    }
    auto sharedState = std::make_shared<FTableSharedState>(
        memoryManager, tableSchema->copy(), hasUnFlatColumn ? 1 : common::DEFAULT_VECTOR_CAPACITY);
    return make_unique<ResultCollector>(std::make_unique<ResultSetDescriptor>(schema),
        tableSchema->copy(), payloadsPos, sharedState, std::move(prevOperator), getOperatorID(),
        binder::ExpressionUtil::toString(expressionsToCollect));
}

std::vector<DataPos> PlanMapper::getExpressionsDataPos(
    const binder::expression_vector& expressions, const planner::Schema& schema) {
    std::vector<DataPos> result;
    for (auto& expression : expressions) {
        result.emplace_back(schema.getExpressionPos(*expression));
    }
    return result;
}

} // namespace processor
} // namespace kuzu
