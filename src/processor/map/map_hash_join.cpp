#include "planner/operator/logical_hash_join.h"
#include "processor/operator/hash_join/hash_join_build.h"
#include "processor/operator/hash_join/hash_join_probe.h"
#include "processor/plan_mapper.h"

using namespace kuzu::binder;
using namespace kuzu::planner;
using namespace kuzu::common;

namespace kuzu {
namespace processor {

std::unique_ptr<HashJoinBuildInfo> PlanMapper::createHashBuildInfo(const Schema& buildSchema,
    const expression_vector& keys, const expression_vector& payloads) {
    planner::f_group_pos_set keyGroupPosSet;
    std::vector<DataPos> keysPos;
    std::vector<FStateType> fStateTypes;
    std::vector<DataPos> payloadsPos;
    auto tableSchema = FactorizedTableSchema();
    for (auto& key : keys) {
        auto pos = DataPos(buildSchema.getExpressionPos(*key));
        keyGroupPosSet.insert(pos.dataChunkPos);
        // Keys are always stored in flat column.
        auto columnSchema = ColumnSchema(false /* isUnFlat */, pos.dataChunkPos,
            LogicalTypeUtils::getRowLayoutSize(key->dataType));
        tableSchema.appendColumn(std::move(columnSchema));
        keysPos.push_back(pos);
        fStateTypes.push_back(buildSchema.getGroup(pos.dataChunkPos)->isFlat() ?
                                  FStateType::FLAT :
                                  FStateType::UNFLAT);
    }
    for (auto& payload : payloads) {
        auto pos = DataPos(buildSchema.getExpressionPos(*payload));
        if (keyGroupPosSet.contains(pos.dataChunkPos) ||
            buildSchema.getGroup(pos.dataChunkPos)->isFlat()) {
            // Payloads need to be stored in flat column in 2 cases
            // 1. payload is in the same chunk as a key. Since keys are always stored as flat,
            // payloads must also be stored as flat.
            // 2. payload is in flat chunk
            auto columnSchema = ColumnSchema(false /* isUnFlat */, pos.dataChunkPos,
                LogicalTypeUtils::getRowLayoutSize(payload->dataType));
            tableSchema.appendColumn(std::move(columnSchema));
        } else {
            auto columnSchema = ColumnSchema(true /* isUnFlat */, pos.dataChunkPos,
                (uint32_t)sizeof(overflow_value_t));
            tableSchema.appendColumn(std::move(columnSchema));
        }
        payloadsPos.push_back(pos);
    }
    auto hashValueColumn = ColumnSchema(false /* isUnFlat */, INVALID_DATA_CHUNK_POS,
        LogicalTypeUtils::getRowLayoutSize(LogicalType::HASH()));
    tableSchema.appendColumn(std::move(hashValueColumn));
    auto pointerColumn = ColumnSchema(false /* isUnFlat */, INVALID_DATA_CHUNK_POS,
        LogicalTypeUtils::getRowLayoutSize(LogicalType::INT64()));
    tableSchema.appendColumn(std::move(pointerColumn));
    return std::make_unique<HashJoinBuildInfo>(std::move(keysPos), std::move(fStateTypes),
        std::move(payloadsPos), std::move(tableSchema));
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapHashJoin(LogicalOperator* logicalOperator) {
    auto hashJoin = (LogicalHashJoin*)logicalOperator;
    auto outSchema = hashJoin->getSchema();
    auto buildSchema = hashJoin->getChild(1)->getSchema();
    std::unique_ptr<PhysicalOperator> probeSidePrevOperator;
    std::unique_ptr<PhysicalOperator> buildSidePrevOperator;
    // Map the side into which semi mask is passed first.
    if (hashJoin->getSIPInfo().dependency == SIPDependency::PROBE_DEPENDS_ON_BUILD) {
        buildSidePrevOperator = mapOperator(hashJoin->getChild(1).get());
        probeSidePrevOperator = mapOperator(hashJoin->getChild(0).get());
    } else {
        probeSidePrevOperator = mapOperator(hashJoin->getChild(0).get());
        buildSidePrevOperator = mapOperator(hashJoin->getChild(1).get());
    }
    expression_vector probeKeys;
    expression_vector buildKeys;
    for (auto& [probeKey, buildKey] : hashJoin->getJoinConditions()) {
        probeKeys.push_back(probeKey);
        buildKeys.push_back(buildKey);
    }
    auto buildKeyTypes = ExpressionUtil::getDataTypes(buildKeys);
    auto payloads =
        ExpressionUtil::excludeExpressions(hashJoin->getExpressionsToMaterialize(), probeKeys);
    // Create build
    auto buildInfo = createHashBuildInfo(*buildSchema, buildKeys, payloads);
    auto globalHashTable = std::make_unique<JoinHashTable>(*clientContext->getMemoryManager(),
        LogicalType::copy(buildKeyTypes), buildInfo->getTableSchema()->copy());
    auto sharedState = std::make_shared<HashJoinSharedState>(std::move(globalHashTable));
    auto printInfo = std::make_unique<OPPrintInfo>(hashJoin->getExpressionsForPrinting());
    auto hashJoinBuild =
        make_unique<HashJoinBuild>(std::make_unique<ResultSetDescriptor>(buildSchema),
            PhysicalOperatorType::HASH_JOIN_BUILD, sharedState, std::move(buildInfo),
            std::move(buildSidePrevOperator), getOperatorID(), printInfo->copy());
    // Create probe
    std::vector<DataPos> probeKeysDataPos;
    for (auto& probeKey : probeKeys) {
        probeKeysDataPos.emplace_back(outSchema->getExpressionPos(*probeKey));
    }
    std::vector<DataPos> probePayloadsOutPos;
    for (auto& payload : payloads) {
        probePayloadsOutPos.emplace_back(outSchema->getExpressionPos(*payload));
    }
    ProbeDataInfo probeDataInfo(probeKeysDataPos, probePayloadsOutPos);
    if (hashJoin->hasMark()) {
        auto mark = hashJoin->getMark();
        auto markOutputPos = DataPos(outSchema->getExpressionPos(*mark));
        probeDataInfo.markDataPos = markOutputPos;
    } else {
        probeDataInfo.markDataPos = DataPos::getInvalidPos();
    }
    auto hashJoinProbe = make_unique<HashJoinProbe>(sharedState, hashJoin->getJoinType(),
        hashJoin->requireFlatProbeKeys(), probeDataInfo, std::move(probeSidePrevOperator),
        std::move(hashJoinBuild), getOperatorID(), printInfo->copy());
    if (hashJoin->getSIPInfo().direction == SIPDirection::PROBE_TO_BUILD) {
        mapSIPJoin(hashJoinProbe.get());
    }
    return hashJoinProbe;
}

} // namespace processor
} // namespace kuzu
