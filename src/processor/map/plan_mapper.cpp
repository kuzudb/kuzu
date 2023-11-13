#include "processor/plan_mapper.h"

#include "processor/operator/profile.h"

using namespace kuzu::common;
using namespace kuzu::planner;

namespace kuzu {
namespace processor {

static void setPhysicalPlanIfProfile(
    planner::LogicalPlan* logicalPlan, PhysicalPlan* physicalPlan) {
    if (logicalPlan->isProfile()) {
        reinterpret_cast<Profile*>(physicalPlan->lastOperator->getChild(0))
            ->setPhysicalPlan(physicalPlan);
    }
}

std::unique_ptr<PhysicalPlan> PlanMapper::mapLogicalPlanToPhysical(
    LogicalPlan* logicalPlan, const binder::expression_vector& expressionsToCollect) {
    auto lastOperator = mapOperator(logicalPlan->getLastOperator().get());
    lastOperator = createResultCollector(AccumulateType::REGULAR, expressionsToCollect,
        logicalPlan->getSchema(), std::move(lastOperator));
    auto physicalPlan = make_unique<PhysicalPlan>(std::move(lastOperator));
    setPhysicalPlanIfProfile(logicalPlan, physicalPlan.get());
    return physicalPlan;
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapOperator(LogicalOperator* logicalOperator) {
    std::unique_ptr<PhysicalOperator> physicalOperator;
    auto operatorType = logicalOperator->getOperatorType();
    switch (operatorType) {
    case planner::LogicalOperatorType::SCAN_FILE: {
        physicalOperator = mapScanFile(logicalOperator);
    } break;
    case LogicalOperatorType::SCAN_FRONTIER: {
        physicalOperator = mapScanFrontier(logicalOperator);
    } break;
    case LogicalOperatorType::SCAN_INTERNAL_ID: {
        physicalOperator = mapScanInternalID(logicalOperator);
    } break;
    case LogicalOperatorType::FILL_TABLE_ID: {
        physicalOperator = mapFillTableID(logicalOperator);
    } break;
    case LogicalOperatorType::INDEX_SCAN_NODE: {
        physicalOperator = mapIndexScan(logicalOperator);
    } break;
    case LogicalOperatorType::UNWIND: {
        physicalOperator = mapUnwind(logicalOperator);
    } break;
    case LogicalOperatorType::EXTEND: {
        physicalOperator = mapExtend(logicalOperator);
    } break;
    case LogicalOperatorType::RECURSIVE_EXTEND: {
        physicalOperator = mapRecursiveExtend(logicalOperator);
    } break;
    case LogicalOperatorType::PATH_PROPERTY_PROBE: {
        physicalOperator = mapPathPropertyProbe(logicalOperator);
    } break;
    case LogicalOperatorType::FLATTEN: {
        physicalOperator = mapFlatten(logicalOperator);
    } break;
    case LogicalOperatorType::FILTER: {
        physicalOperator = mapFilter(logicalOperator);
    } break;
    case LogicalOperatorType::PROJECTION: {
        physicalOperator = mapProjection(logicalOperator);
    } break;
    case LogicalOperatorType::SEMI_MASKER: {
        physicalOperator = mapSemiMasker(logicalOperator);
    } break;
    case LogicalOperatorType::HASH_JOIN: {
        physicalOperator = mapHashJoin(logicalOperator);
    } break;
    case LogicalOperatorType::INTERSECT: {
        physicalOperator = mapIntersect(logicalOperator);
    } break;
    case LogicalOperatorType::CROSS_PRODUCT: {
        physicalOperator = mapCrossProduct(logicalOperator);
    } break;
    case LogicalOperatorType::SCAN_NODE_PROPERTY: {
        physicalOperator = mapScanNodeProperty(logicalOperator);
    } break;
    case LogicalOperatorType::MULTIPLICITY_REDUCER: {
        physicalOperator = mapMultiplicityReducer(logicalOperator);
    } break;
    case LogicalOperatorType::NODE_LABEL_FILTER: {
        physicalOperator = mapNodeLabelFilter(logicalOperator);
    } break;
    case LogicalOperatorType::LIMIT: {
        physicalOperator = mapLimit(logicalOperator);
    } break;
    case LogicalOperatorType::MERGE: {
        physicalOperator = mapMerge(logicalOperator);
    } break;
    case LogicalOperatorType::AGGREGATE: {
        physicalOperator = mapAggregate(logicalOperator);
    } break;
    case LogicalOperatorType::DISTINCT: {
        physicalOperator = mapDistinct(logicalOperator);
    } break;
    case LogicalOperatorType::ORDER_BY: {
        physicalOperator = mapOrderBy(logicalOperator);
    } break;
    case LogicalOperatorType::UNION_ALL: {
        physicalOperator = mapUnionAll(logicalOperator);
    } break;
    case LogicalOperatorType::ACCUMULATE: {
        physicalOperator = mapAccumulate(logicalOperator);
    } break;
    case LogicalOperatorType::DUMMY_SCAN: {
        physicalOperator = mapDummyScan(logicalOperator);
    } break;
    case LogicalOperatorType::INSERT_NODE: {
        physicalOperator = mapInsertNode(logicalOperator);
    } break;
    case LogicalOperatorType::INSERT_REL: {
        physicalOperator = mapInsertRel(logicalOperator);
    } break;
    case LogicalOperatorType::SET_NODE_PROPERTY: {
        physicalOperator = mapSetNodeProperty(logicalOperator);
    } break;
    case LogicalOperatorType::SET_REL_PROPERTY: {
        physicalOperator = mapSetRelProperty(logicalOperator);
    } break;
    case LogicalOperatorType::DELETE_NODE: {
        physicalOperator = mapDeleteNode(logicalOperator);
    } break;
    case LogicalOperatorType::DELETE_REL: {
        physicalOperator = mapDeleteRel(logicalOperator);
    } break;
    case LogicalOperatorType::CREATE_TABLE: {
        physicalOperator = mapCreateTable(logicalOperator);
    } break;
    case LogicalOperatorType::COPY_FROM: {
        physicalOperator = mapCopyFrom(logicalOperator);
    } break;
    case LogicalOperatorType::PARTITIONER: {
        physicalOperator = mapPartitioner(logicalOperator);
    } break;
    case LogicalOperatorType::COPY_TO: {
        physicalOperator = mapCopyTo(logicalOperator);
    } break;
    case LogicalOperatorType::DROP_TABLE: {
        physicalOperator = mapDropTable(logicalOperator);
    } break;
    case LogicalOperatorType::ALTER: {
        physicalOperator = mapAlter(logicalOperator);
    } break;
    case LogicalOperatorType::STANDALONE_CALL: {
        physicalOperator = mapStandaloneCall(logicalOperator);
    } break;
    case LogicalOperatorType::COMMENT_ON: {
        physicalOperator = mapCommentOn(logicalOperator);
    } break;
    case LogicalOperatorType::IN_QUERY_CALL: {
        physicalOperator = mapInQueryCall(logicalOperator);
    } break;
    case LogicalOperatorType::EXPLAIN: {
        physicalOperator = mapExplain(logicalOperator);
    } break;
    case LogicalOperatorType::EXPRESSIONS_SCAN: {
        physicalOperator = mapExpressionsScan(logicalOperator);
    } break;
    case LogicalOperatorType::CREATE_MACRO: {
        physicalOperator = mapCreateMacro(logicalOperator);
    } break;
    case LogicalOperatorType::TRANSACTION: {
        physicalOperator = mapTransaction(logicalOperator);
    } break;
    default:
        KU_UNREACHABLE;
    }
    logicalOpToPhysicalOpMap.insert({logicalOperator, physicalOperator.get()});
    return physicalOperator;
}

std::vector<DataPos> PlanMapper::getExpressionsDataPos(
    const binder::expression_vector& expressions, const planner::Schema& schema) {
    std::vector<DataPos> result;
    for (auto& expression : expressions) {
        result.emplace_back(schema.getExpressionPos(*expression));
    }
    return result;
}

std::shared_ptr<FactorizedTable> PlanMapper::getSingleStringColumnFTable() {
    auto ftTableSchema = std::make_unique<FactorizedTableSchema>();
    ftTableSchema->appendColumn(
        std::make_unique<ColumnSchema>(false /* flat */, 0 /* dataChunkPos */,
            LogicalTypeUtils::getRowLayoutSize(LogicalType{LogicalTypeID::STRING})));
    auto fTable = std::make_shared<FactorizedTable>(memoryManager, std::move(ftTableSchema));
    return fTable;
}

} // namespace processor
} // namespace kuzu
