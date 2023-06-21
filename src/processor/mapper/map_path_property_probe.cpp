#include "common/string_utils.h"
#include "planner/logical_plan/logical_operator/logical_recursive_extend.h"
#include "processor/mapper/plan_mapper.h"
#include "processor/operator/hash_join/hash_join_build.h"
#include "processor/operator/recursive_extend/path_property_probe.h"

using namespace kuzu::binder;
using namespace kuzu::planner;

namespace kuzu {
namespace processor {

static std::vector<ft_col_idx_t> getColIdxToScan(
    const expression_vector& payloads, uint32_t numKeys, const common::LogicalType& structType) {
    std::unordered_map<std::string, ft_col_idx_t> propertyNameToColumnIdx;
    for (auto i = 0u; i < payloads.size(); ++i) {
        assert(payloads[i]->expressionType == common::PROPERTY);
        auto propertyName = ((PropertyExpression*)payloads[i].get())->getPropertyName();
        common::StringUtils::toUpper(propertyName);
        propertyNameToColumnIdx.insert({propertyName, i + numKeys});
    }
    auto nodeStructFields = common::StructType::getFields(&structType);
    std::vector<ft_col_idx_t> colIndicesToScan;
    for (auto i = 1u; i < nodeStructFields.size(); ++i) {
        auto field = nodeStructFields[i];
        colIndicesToScan.push_back(propertyNameToColumnIdx.at(field->getName()));
    }
    return colIndicesToScan;
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapLogicalPathPropertyProbeToPhysical(
    planner::LogicalOperator* logicalOperator) {
    auto logicalProbe = (LogicalPathPropertyProbe*)logicalOperator;
    if (logicalProbe->getNumChildren() == 1) {
        return mapLogicalOperatorToPhysical(logicalProbe->getChild(0));
    }
    auto rel = logicalProbe->getRel();
    auto recursiveInfo = rel->getRecursiveInfo();
    // Map build node property
    auto nodeBuildPrevOperator = mapLogicalOperatorToPhysical(logicalProbe->getChild(1));
    auto nodeBuildSchema = logicalProbe->getChild(1)->getSchema();
    auto nodeKeys = expression_vector{recursiveInfo->node->getInternalIDProperty()};
    auto nodePayloads =
        ExpressionUtil::excludeExpressions(nodeBuildSchema->getExpressionsInScope(), nodeKeys);
    auto nodeBuildInfo = createHashBuildInfo(*nodeBuildSchema, nodeKeys, nodePayloads);
    auto nodeHashTable = std::make_unique<JoinHashTable>(
        *memoryManager, nodeBuildInfo->getNumKeys(), nodeBuildInfo->getTableSchema()->copy());
    auto nodeBuildSharedState = std::make_shared<HashJoinSharedState>(std::move(nodeHashTable));
    auto nodeBuild = make_unique<HashJoinBuild>(
        std::make_unique<ResultSetDescriptor>(nodeBuildSchema), nodeBuildSharedState,
        std::move(nodeBuildInfo), std::move(nodeBuildPrevOperator), getOperatorID(), "");
    // Map build rel property
    auto relBuildPrvOperator = mapLogicalOperatorToPhysical(logicalProbe->getChild(2));
    auto relBuildSchema = logicalProbe->getChild(2)->getSchema();
    auto relKeys = expression_vector{recursiveInfo->rel->getInternalIDProperty()};
    auto relPayloads =
        ExpressionUtil::excludeExpressions(relBuildSchema->getExpressionsInScope(), relKeys);
    auto relBuildInfo = createHashBuildInfo(*relBuildSchema, relKeys, relPayloads);
    auto relHashTable = std::make_unique<JoinHashTable>(
        *memoryManager, relBuildInfo->getNumKeys(), relBuildInfo->getTableSchema()->copy());
    auto relBuildSharedState = std::make_shared<HashJoinSharedState>(std::move(relHashTable));
    auto relBuild = std::make_unique<HashJoinBuild>(
        std::make_unique<ResultSetDescriptor>(relBuildSchema), relBuildSharedState,
        std::move(relBuildInfo), std::move(relBuildPrvOperator), getOperatorID(), "");
    // Map child
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->getChild(0));
    // Map probe
    auto relDataType = rel->getDataType();
    auto nodesField = common::StructType::getField(&relDataType, common::InternalKeyword::NODES);
    auto nodeStructType = common::VarListType::getChildType(nodesField->getType());
    auto nodeColIndicesToScan = getColIdxToScan(nodePayloads, nodeKeys.size(), *nodeStructType);
    auto relsField = common::StructType::getField(&relDataType, common::InternalKeyword::RELS);
    auto relStructType = common::VarListType::getChildType(relsField->getType());
    auto relColIndicesToScan = getColIdxToScan(relPayloads, relKeys.size(), *relStructType);
    auto pathPos = DataPos{logicalProbe->getSchema()->getExpressionPos(*rel)};
    auto pathProbeInfo = std::make_unique<PathPropertyProbeDataInfo>(
        pathPos, std::move(nodeColIndicesToScan), std::move(relColIndicesToScan));
    auto pathProbeSharedState =
        std::make_shared<PathPropertyProbeSharedState>(nodeBuildSharedState, relBuildSharedState);
    std::vector<std::unique_ptr<PhysicalOperator>> children;
    children.push_back(std::move(prevOperator));
    children.push_back(std::move(nodeBuild));
    children.push_back(std::move(relBuild));
    auto pathPropertyProbe = std::make_unique<PathPropertyProbe>(
        std::move(pathProbeInfo), pathProbeSharedState, std::move(children), getOperatorID(), "");
    if (logicalProbe->getSIP() == planner::SidewaysInfoPassing::PROBE_TO_BUILD) {
        mapSIPJoin(pathPropertyProbe.get());
    }
    return pathPropertyProbe;
}

} // namespace processor
} // namespace kuzu
