#include "common/string_utils.h"
#include "planner/logical_plan/extend/logical_recursive_extend.h"
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
        if (payloads[i]->expressionType == PROPERTY) {
            auto propertyName = ((PropertyExpression*)payloads[i].get())->getPropertyName();
            StringUtils::toUpper(propertyName);
            propertyNameToColumnIdx.insert({propertyName, i + numKeys});
        } else { // label expression
            propertyNameToColumnIdx.insert({InternalKeyword::LABEL, i + numKeys});
        }
    }
    auto structFields = StructType::getFields(&structType);
    std::vector<struct_field_idx_t> structFieldIndices;
    std::vector<ft_col_idx_t> colIndices;
    for (auto i = 0u; i < structFields.size(); ++i) {
        auto field = structFields[i];
        auto fieldName = StringUtils::getUpper(field->getName());
        if (propertyNameToColumnIdx.contains(fieldName)) {
            structFieldIndices.push_back(i);
            colIndices.push_back(propertyNameToColumnIdx.at(fieldName));
        }
    }
    return std::make_pair(std::move(structFieldIndices), std::move(colIndices));
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapPathPropertyProbe(
    planner::LogicalOperator* logicalOperator) {
    auto logicalProbe = (LogicalPathPropertyProbe*)logicalOperator;
    if (logicalProbe->getNumChildren() == 1) {
        return mapOperator(logicalProbe->getChild(0).get());
    }
    auto rel = logicalProbe->getRel();
    auto recursiveInfo = rel->getRecursiveInfo();
    // Map build node property
    auto nodeBuildPrevOperator = mapOperator(logicalProbe->getChild(1).get());
    auto nodeBuildSchema = logicalProbe->getChild(1)->getSchema();
    auto nodeKeys = expression_vector{recursiveInfo->node->getInternalIDProperty()};
    auto nodeKeyTypes = ExpressionUtil::getDataTypes(nodeKeys);
    auto nodePayloads =
        ExpressionUtil::excludeExpressions(nodeBuildSchema->getExpressionsInScope(), nodeKeys);
    auto nodeBuildInfo = createHashBuildInfo(*nodeBuildSchema, nodeKeys, nodePayloads);
    auto nodeHashTable = std::make_unique<JoinHashTable>(
        *memoryManager, std::move(nodeKeyTypes), nodeBuildInfo->getTableSchema()->copy());
    auto nodeBuildSharedState = std::make_shared<HashJoinSharedState>(std::move(nodeHashTable));
    auto nodeBuild = make_unique<HashJoinBuild>(
        std::make_unique<ResultSetDescriptor>(nodeBuildSchema), nodeBuildSharedState,
        std::move(nodeBuildInfo), std::move(nodeBuildPrevOperator), getOperatorID(), "");
    // Map build rel property
    auto relBuildPrvOperator = mapOperator(logicalProbe->getChild(2).get());
    auto relBuildSchema = logicalProbe->getChild(2)->getSchema();
    auto relKeys = expression_vector{recursiveInfo->rel->getInternalIDProperty()};
    auto relKeyTypes = ExpressionUtil::getDataTypes(relKeys);
    auto relPayloads =
        ExpressionUtil::excludeExpressions(relBuildSchema->getExpressionsInScope(), relKeys);
    auto relBuildInfo = createHashBuildInfo(*relBuildSchema, relKeys, relPayloads);
    auto relHashTable = std::make_unique<JoinHashTable>(
        *memoryManager, std::move(relKeyTypes), relBuildInfo->getTableSchema()->copy());
    auto relBuildSharedState = std::make_shared<HashJoinSharedState>(std::move(relHashTable));
    auto relBuild = std::make_unique<HashJoinBuild>(
        std::make_unique<ResultSetDescriptor>(relBuildSchema), relBuildSharedState,
        std::move(relBuildInfo), std::move(relBuildPrvOperator), getOperatorID(), "");
    // Map child
    auto prevOperator = mapOperator(logicalOperator->getChild(0).get());
    // Map probe
    auto relDataType = rel->getDataType();
    auto nodesField = StructType::getField(&relDataType, InternalKeyword::NODES);
    auto nodeStructType = VarListType::getChildType(nodesField->getType());
    auto [nodeFieldIndices, nodeTableColumnIndices] =
        getColIdxToScan(nodePayloads, nodeKeys.size(), *nodeStructType);
    auto relsField = StructType::getField(&relDataType, InternalKeyword::RELS);
    auto relStructType = VarListType::getChildType(relsField->getType());
    auto [relFieldIndices, relTableColumnIndices] =
        getColIdxToScan(relPayloads, relKeys.size(), *relStructType);
    auto pathPos = DataPos{logicalProbe->getSchema()->getExpressionPos(*rel)};
    auto pathProbeInfo = std::make_unique<PathPropertyProbeDataInfo>(pathPos,
        std::move(nodeFieldIndices), std::move(relFieldIndices), std::move(nodeTableColumnIndices),
        std::move(relTableColumnIndices));
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
