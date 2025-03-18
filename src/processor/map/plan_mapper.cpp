#include "processor/plan_mapper.h"

#include "binder/expression/node_expression.h"
#include "planner/operator/sip/logical_semi_masker.h"
#include "processor/operator/profile.h"
#include "storage/storage_manager.h"
#include "storage/store/node_table.h"

using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::planner;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

static void setPhysicalPlanIfProfile(const LogicalPlan* logicalPlan, PhysicalPlan* physicalPlan) {
    if (logicalPlan->isProfile()) {
        ku_dynamic_cast<Profile*>(physicalPlan->lastOperator->getChild(0))
            ->setPhysicalPlan(physicalPlan);
    }
}

std::unique_ptr<PhysicalPlan> PlanMapper::mapLogicalPlanToPhysical(const LogicalPlan* logicalPlan,
    const expression_vector& expressionsToCollect) {
    auto lastOperator = mapOperator(logicalPlan->getLastOperator().get());
    lastOperator = createResultCollector(AccumulateType::REGULAR, expressionsToCollect,
        logicalPlan->getSchema(), std::move(lastOperator));
    auto physicalPlan = make_unique<PhysicalPlan>(std::move(lastOperator));
    setPhysicalPlanIfProfile(logicalPlan, physicalPlan.get());
    return physicalPlan;
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapOperator(const LogicalOperator* logicalOperator) {
    std::unique_ptr<PhysicalOperator> physicalOperator;
    switch (logicalOperator->getOperatorType()) {
    case LogicalOperatorType::ACCUMULATE: {
        physicalOperator = mapAccumulate(logicalOperator);
    } break;
    case LogicalOperatorType::AGGREGATE: {
        physicalOperator = mapAggregate(logicalOperator);
    } break;
    case LogicalOperatorType::ALTER: {
        physicalOperator = mapAlter(logicalOperator);
    } break;
    case LogicalOperatorType::ATTACH_DATABASE: {
        physicalOperator = mapAttachDatabase(logicalOperator);
    } break;
    case LogicalOperatorType::COPY_FROM: {
        physicalOperator = mapCopyFrom(logicalOperator);
    } break;
    case LogicalOperatorType::COPY_TO: {
        physicalOperator = mapCopyTo(logicalOperator);
    } break;
    case LogicalOperatorType::CREATE_MACRO: {
        physicalOperator = mapCreateMacro(logicalOperator);
    } break;
    case LogicalOperatorType::CREATE_SEQUENCE: {
        physicalOperator = mapCreateSequence(logicalOperator);
    } break;
    case LogicalOperatorType::CREATE_TABLE: {
        physicalOperator = mapCreateTable(logicalOperator);
    } break;
    case LogicalOperatorType::CREATE_TYPE: {
        physicalOperator = mapCreateType(logicalOperator);
    } break;
    case LogicalOperatorType::CROSS_PRODUCT: {
        physicalOperator = mapCrossProduct(logicalOperator);
    } break;
    case LogicalOperatorType::DELETE: {
        physicalOperator = mapDelete(logicalOperator);
    } break;
    case LogicalOperatorType::DETACH_DATABASE: {
        physicalOperator = mapDetachDatabase(logicalOperator);
    } break;
    case LogicalOperatorType::DISTINCT: {
        physicalOperator = mapDistinct(logicalOperator);
    } break;
    case LogicalOperatorType::DROP: {
        physicalOperator = mapDrop(logicalOperator);
    } break;
    case LogicalOperatorType::DUMMY_SCAN: {
        physicalOperator = mapDummyScan(logicalOperator);
    } break;
    case LogicalOperatorType::DUMMY_SINK: {
        physicalOperator = mapDummySink(logicalOperator);
    } break;
    case LogicalOperatorType::EMPTY_RESULT: {
        physicalOperator = mapEmptyResult(logicalOperator);
    } break;
    case LogicalOperatorType::EXPLAIN: {
        physicalOperator = mapExplain(logicalOperator);
    } break;
    case LogicalOperatorType::EXPRESSIONS_SCAN: {
        physicalOperator = mapExpressionsScan(logicalOperator);
    } break;
    case LogicalOperatorType::EXTEND: {
        physicalOperator = mapExtend(logicalOperator);
    } break;
    case LogicalOperatorType::EXTENSION: {
        physicalOperator = mapExtension(logicalOperator);
    } break;
    case LogicalOperatorType::EXPORT_DATABASE: {
        physicalOperator = mapExportDatabase(logicalOperator);
    } break;
    case LogicalOperatorType::FLATTEN: {
        physicalOperator = mapFlatten(logicalOperator);
    } break;
    case LogicalOperatorType::FILTER: {
        physicalOperator = mapFilter(logicalOperator);
    } break;
    case LogicalOperatorType::HASH_JOIN: {
        physicalOperator = mapHashJoin(logicalOperator);
    } break;
    case LogicalOperatorType::IMPORT_DATABASE: {
        physicalOperator = mapImportDatabase(logicalOperator);
    } break;
    case LogicalOperatorType::INDEX_LOOK_UP: {
        physicalOperator = mapIndexLookup(logicalOperator);
    } break;
    case LogicalOperatorType::INTERSECT: {
        physicalOperator = mapIntersect(logicalOperator);
    } break;
    case LogicalOperatorType::INSERT: {
        physicalOperator = mapInsert(logicalOperator);
    } break;
    case LogicalOperatorType::LIMIT: {
        physicalOperator = mapLimit(logicalOperator);
    } break;
    case LogicalOperatorType::MERGE: {
        physicalOperator = mapMerge(logicalOperator);
    } break;
    case LogicalOperatorType::MULTIPLICITY_REDUCER: {
        physicalOperator = mapMultiplicityReducer(logicalOperator);
    } break;
    case LogicalOperatorType::NODE_LABEL_FILTER: {
        physicalOperator = mapNodeLabelFilter(logicalOperator);
    } break;
    case LogicalOperatorType::ORDER_BY: {
        physicalOperator = mapOrderBy(logicalOperator);
    } break;
    case LogicalOperatorType::PARTITIONER: {
        physicalOperator = mapPartitioner(logicalOperator);
    } break;
    case LogicalOperatorType::PATH_PROPERTY_PROBE: {
        physicalOperator = mapPathPropertyProbe(logicalOperator);
    } break;
    case LogicalOperatorType::PROJECTION: {
        physicalOperator = mapProjection(logicalOperator);
    } break;
    case LogicalOperatorType::RECURSIVE_EXTEND: {
        physicalOperator = mapRecursiveExtend(logicalOperator);
    } break;
    case LogicalOperatorType::SCAN_NODE_TABLE: {
        physicalOperator = mapScanNodeTable(logicalOperator);
    } break;
    case LogicalOperatorType::SEMI_MASKER: {
        physicalOperator = mapSemiMasker(logicalOperator);
    } break;
    case LogicalOperatorType::SET_PROPERTY: {
        physicalOperator = mapSetProperty(logicalOperator);
    } break;
    case LogicalOperatorType::STANDALONE_CALL: {
        physicalOperator = mapStandaloneCall(logicalOperator);
    } break;
    case LogicalOperatorType::TABLE_FUNCTION_CALL: {
        physicalOperator = mapTableFunctionCall(logicalOperator);
    } break;
    case LogicalOperatorType::TRANSACTION: {
        physicalOperator = mapTransaction(logicalOperator);
    } break;
    case LogicalOperatorType::UNION_ALL: {
        physicalOperator = mapUnionAll(logicalOperator);
    } break;
    case LogicalOperatorType::UNWIND: {
        physicalOperator = mapUnwind(logicalOperator);
    } break;
    case LogicalOperatorType::USE_DATABASE: {
        physicalOperator = mapUseDatabase(logicalOperator);
    } break;
    default:
        KU_UNREACHABLE;
    }
    if (!logicalOpToPhysicalOpMap.contains(logicalOperator)) {
        logicalOpToPhysicalOpMap.insert({logicalOperator, physicalOperator.get()});
    }
    return physicalOperator;
}

std::vector<DataPos> PlanMapper::getDataPos(const expression_vector& expressions,
    const Schema& schema) {
    std::vector<DataPos> result;
    for (auto& expression : expressions) {
        result.emplace_back(getDataPos(*expression, schema));
    }
    return result;
}

FactorizedTableSchema PlanMapper::createFlatFTableSchema(const expression_vector& expressions,
    const Schema& schema) {
    auto tableSchema = FactorizedTableSchema();
    for (auto& expr : expressions) {
        auto dataPos = getDataPos(*expr, schema);
        auto columnSchema = ColumnSchema(false /* isUnFlat */, dataPos.dataChunkPos,
            LogicalTypeUtils::getRowLayoutSize(expr->getDataType()));
        tableSchema.appendColumn(std::move(columnSchema));
    }
    return tableSchema;
}

std::unique_ptr<SemiMask> PlanMapper::createSemiMask(table_id_t tableID) const {
    auto table = clientContext->getStorageManager()->getTable(tableID)->ptrCast<NodeTable>();
    return SemiMaskUtil::createMask(table->getNumTotalRows(clientContext->getTransaction()));
}

std::unique_ptr<NodeOffsetMaskMap> PlanMapper::createNodeOffsetMaskMap(
    const Expression& expr) const {
    auto& node = expr.constCast<NodeExpression>();
    auto maskMap = std::make_unique<NodeOffsetMaskMap>();
    for (auto tableID : node.getTableIDs()) {
        maskMap->addMask(tableID, createSemiMask(tableID));
    }
    return maskMap;
}

LogicalSemiMasker* PlanMapper::findSemiMaskerInPlan(LogicalOperator* logicalOperator) {
    if (logicalOperator->getOperatorType() == LogicalOperatorType::SEMI_MASKER) {
        return logicalOperator->ptrCast<LogicalSemiMasker>();
    }
    for (auto& child : logicalOperator->getChildren()) {
        const auto semiMasker = findSemiMaskerInPlan(child.get());
        if (semiMasker) {
            return semiMasker;
        }
    }
    return nullptr;
}

} // namespace processor
} // namespace kuzu
