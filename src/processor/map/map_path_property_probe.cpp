#include "binder/expression/expression_util.h"
#include "binder/expression/property_expression.h"
#include "catalog/catalog_entry/table_catalog_entry.h"
#include "common/string_utils.h"
#include "planner/operator/extend/logical_recursive_extend.h"
#include "planner/operator/logical_gds_call.h"
#include "processor/operator/gds_call.h"
#include "processor/operator/hash_join/hash_join_build.h"
#include "processor/operator/recursive_extend/path_property_probe.h"
#include "processor/plan_mapper.h"

using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::planner;

namespace kuzu {
namespace processor {

static std::pair<std::vector<struct_field_idx_t>, std::vector<ft_col_idx_t>> getColIdxToScan(
    const expression_vector& payloads, uint32_t numKeys, const LogicalType& structType) {
    std::unordered_map<std::string, ft_col_idx_t> propertyNameToColumnIdx;
    for (auto i = 0u; i < payloads.size(); ++i) {
        KU_ASSERT(payloads[i]->expressionType == ExpressionType::PROPERTY);
        auto propertyName = ((PropertyExpression*)payloads[i].get())->getPropertyName();
        StringUtils::toUpper(propertyName);
        propertyNameToColumnIdx.insert({propertyName, i + numKeys});
    }
    const auto& structFields = StructType::getFields(structType);
    std::vector<struct_field_idx_t> structFieldIndices;
    std::vector<ft_col_idx_t> colIndices;
    for (auto i = 0u; i < structFields.size(); ++i) {
        auto field = structFields[i].copy();
        auto fieldName = StringUtils::getUpper(field.getName());
        if (propertyNameToColumnIdx.contains(fieldName)) {
            structFieldIndices.push_back(i);
            colIndices.push_back(propertyNameToColumnIdx.at(fieldName));
        }
    }
    return std::make_pair(std::move(structFieldIndices), std::move(colIndices));
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapPathPropertyProbe(
    planner::LogicalOperator* logicalOperator) {
    auto& logicalProbe = logicalOperator->constCast<LogicalPathPropertyProbe>();
    if (logicalProbe.getJoinType() == RecursiveJoinType::TRACK_NONE) {
        return mapOperator(logicalProbe.getChild(0).get());
    }
    auto rel = logicalProbe.getRel();
    auto recursiveInfo = rel->getRecursiveInfo();
    std::vector<common::struct_field_idx_t> nodeFieldIndices;
    std::vector<ft_col_idx_t> nodeTableColumnIndices;
    std::shared_ptr<HashJoinSharedState> nodeBuildSharedState = nullptr;
    std::unique_ptr<HashJoinBuild> nodeBuild = nullptr;
    // Map build node property
    if (logicalProbe.getNodeChild() != nullptr) {
        auto nodeBuildPrevOperator = mapOperator(logicalProbe.getNodeChild().get());
        auto nodeBuildSchema = logicalProbe.getNodeChild()->getSchema();
        auto nodeKeys = expression_vector{recursiveInfo->node->getInternalID()};
        auto nodeKeyTypes = ExpressionUtil::getDataTypes(nodeKeys);
        auto nodePayloads =
            ExpressionUtil::excludeExpressions(nodeBuildSchema->getExpressionsInScope(), nodeKeys);
        auto nodeBuildInfo = createHashBuildInfo(*nodeBuildSchema, nodeKeys, nodePayloads);
        auto nodeHashTable = std::make_unique<JoinHashTable>(*clientContext->getMemoryManager(),
            std::move(nodeKeyTypes), nodeBuildInfo->getTableSchema()->copy());
        nodeBuildSharedState = std::make_shared<HashJoinSharedState>(std::move(nodeHashTable));
        nodeBuild = make_unique<HashJoinBuild>(
            std::make_unique<ResultSetDescriptor>(nodeBuildSchema),
            PhysicalOperatorType::HASH_JOIN_BUILD, nodeBuildSharedState, std::move(nodeBuildInfo),
            std::move(nodeBuildPrevOperator), getOperatorID(), std::make_unique<OPPrintInfo>());
        auto [fieldIndices, columnIndices] = getColIdxToScan(nodePayloads, nodeKeys.size(),
            ListType::getChildType(
                StructType::getField(rel->getDataType(), InternalKeyword::NODES).getType()));
        nodeFieldIndices = std::move(fieldIndices);
        nodeTableColumnIndices = std::move(columnIndices);
    }
    std::vector<common::struct_field_idx_t> relFieldIndices;
    std::vector<ft_col_idx_t> relTableColumnIndices;
    std::shared_ptr<HashJoinSharedState> relBuildSharedState = nullptr;
    std::unique_ptr<HashJoinBuild> relBuild = nullptr;
    // Map build rel property
    if (logicalProbe.getRelChild() != nullptr) {
        auto relBuildPrvOperator = mapOperator(logicalProbe.getRelChild().get());
        auto relBuildSchema = logicalProbe.getRelChild()->getSchema();
        auto relKeys = expression_vector{recursiveInfo->rel->getInternalIDProperty()};
        auto relKeyTypes = ExpressionUtil::getDataTypes(relKeys);
        auto relPayloads =
            ExpressionUtil::excludeExpressions(relBuildSchema->getExpressionsInScope(), relKeys);
        auto relBuildInfo = createHashBuildInfo(*relBuildSchema, relKeys, relPayloads);
        auto relHashTable = std::make_unique<JoinHashTable>(*clientContext->getMemoryManager(),
            std::move(relKeyTypes), relBuildInfo->getTableSchema()->copy());
        relBuildSharedState = std::make_shared<HashJoinSharedState>(std::move(relHashTable));
        relBuild =
            std::make_unique<HashJoinBuild>(std::make_unique<ResultSetDescriptor>(relBuildSchema),
                PhysicalOperatorType::HASH_JOIN_BUILD, relBuildSharedState, std::move(relBuildInfo),
                std::move(relBuildPrvOperator), getOperatorID(), std::make_unique<OPPrintInfo>());
        auto [fieldIndices, columnIndices] = getColIdxToScan(relPayloads, relKeys.size(),
            ListType::getChildType(
                StructType::getField(rel->getDataType(), InternalKeyword::RELS).getType()));
        relFieldIndices = std::move(fieldIndices);
        relTableColumnIndices = std::move(columnIndices);
    }
    // Map child
    auto logicalChild = logicalOperator->getChild(0).get();
    auto prevOperator = mapOperator(logicalChild);
    if (logicalChild->getOperatorType() == LogicalOperatorType::SEMI_MASKER) {
        // Create a pipeline to populate semi mask. Pipeline source is the scan of GDS result, and
        // pipeline sink is a dummy operator that does not materialize anything.
        prevOperator = std::make_unique<DummySink>(
            std::make_unique<ResultSetDescriptor>(logicalChild->getSchema()),
            std::move(prevOperator), getOperatorID(), std::make_unique<OPPrintInfo>());
        auto call = logicalChild->getChild(0)->ptrCast<LogicalGDSCall>();
        auto columns = call->getInfo().outExprs;
        auto physicalCall = logicalOpToPhysicalOpMap.at(call)->ptrCast<GDSCall>();
        physical_op_vector_t children;
        children.push_back(std::move(prevOperator));
        prevOperator = createFTableScanAligned(columns, call->getSchema(),
            physicalCall->getSharedState()->fTable, DEFAULT_VECTOR_CAPACITY, std::move(children));
    }
    // Map probe
    auto pathProbeInfo = PathPropertyProbeInfo();
    auto schema = logicalProbe.getSchema();
    pathProbeInfo.pathPos = getDataPos(*rel, *schema);
    if (logicalProbe.getPathEdgeIDs() != nullptr) {
        pathProbeInfo.leftNodeIDPos = getDataPos(*rel->getLeftNode()->getInternalID(), *schema);
        pathProbeInfo.rightNodeIDPos = getDataPos(*rel->getRightNode()->getInternalID(), *schema);
        pathProbeInfo.inputNodeIDsPos = getDataPos(*logicalProbe.getPathNodeIDs(), *schema);
        pathProbeInfo.inputEdgeIDsPos = getDataPos(*logicalProbe.getPathEdgeIDs(), *schema);
        pathProbeInfo.extendFromLeft = logicalProbe.extendFromLeft;
        pathProbeInfo.extendDirection = logicalProbe.direction;
        if (logicalProbe.direction == common::ExtendDirection::BOTH) {
            pathProbeInfo.directionPos =
                getDataPos(*recursiveInfo->pathEdgeDirectionsExpr, *schema);
        }
        for (auto entry : recursiveInfo->node->getEntries()) {
            pathProbeInfo.tableIDToName.insert({entry->getTableID(), entry->getName()});
        }
        for (auto& entry : recursiveInfo->rel->getEntries()) {
            pathProbeInfo.tableIDToName.insert({entry->getTableID(), entry->getName()});
        }
    }
    pathProbeInfo.nodeFieldIndices = nodeFieldIndices;
    pathProbeInfo.relFieldIndices = relFieldIndices;
    pathProbeInfo.nodeTableColumnIndices = nodeTableColumnIndices;
    pathProbeInfo.relTableColumnIndices = relTableColumnIndices;
    pathProbeInfo.extendFromLeft = logicalProbe.extendFromLeft;
    auto pathProbeSharedState =
        std::make_shared<PathPropertyProbeSharedState>(nodeBuildSharedState, relBuildSharedState);
    std::vector<std::unique_ptr<PhysicalOperator>> children;
    children.push_back(std::move(prevOperator));
    if (nodeBuild != nullptr) {
        children.push_back(std::move(nodeBuild));
    }
    if (relBuild != nullptr) {
        children.push_back(std::move(relBuild));
    }
    auto printInfo = std::make_unique<OPPrintInfo>();
    auto pathPropertyProbe = std::make_unique<PathPropertyProbe>(std::move(pathProbeInfo),
        pathProbeSharedState, std::move(children), getOperatorID(), std::move(printInfo));
    if (logicalProbe.getSIPInfo().direction == planner::SIPDirection::PROBE_TO_BUILD) {
        mapSIPJoin(pathPropertyProbe.get());
    }
    return pathPropertyProbe;
}

} // namespace processor
} // namespace kuzu
