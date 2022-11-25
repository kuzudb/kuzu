#include "planner/logical_plan/logical_operator/sink_util.h"

namespace kuzu {
namespace planner {

void SinkOperatorUtil::mergeSchema(
    const Schema& inputSchema, Schema& result, const vector<string>& keys) {
    unordered_map<uint32_t, vector<string>> keyGroupPosToKeysMap;
    for (auto& key : keys) {
        auto groupPos = inputSchema.getGroupPos(key);
        keyGroupPosToKeysMap[groupPos].push_back(key);
    }
    for (auto& [keyGroupPos, keysInGroup] : keyGroupPosToKeysMap) {
        mergeKeyGroup(inputSchema, result, keyGroupPos, keysInGroup);
    }
    if (getGroupsPosIgnoringKeyGroups(inputSchema, keys).empty()) { // nothing else to merge
        return;
    }
    auto flatPayloads = getFlatPayloadsIgnoringKeyGroup(inputSchema, keys);
    if (!flatPayloads.empty()) {
        auto flatPayloadsOutputGroupPos = appendPayloadsToNewGroup(result, flatPayloads);
        result.flattenGroup(flatPayloadsOutputGroupPos);
    }
    for (auto& payloadGroupPos : getGroupsPosIgnoringKeyGroups(inputSchema, keys)) {
        auto payloadGroup = inputSchema.getGroup(payloadGroupPos);
        if (!payloadGroup->getIsFlat()) {
            auto payloads = inputSchema.getExpressionsInScope(payloadGroupPos);
            auto outputGroupPos = appendPayloadsToNewGroup(result, payloads);
            result.getGroup(outputGroupPos)->setMultiplier(payloadGroup->getMultiplier());
        }
    }
}

void SinkOperatorUtil::mergeSchema(const Schema& inputSchema, Schema& result) {
    auto flatPayloads = getFlatPayloads(inputSchema);
    if (!hasUnFlatPayload(inputSchema)) {
        appendPayloadsToNewGroup(result, flatPayloads);
    } else {
        if (!flatPayloads.empty()) {
            auto flatPayloadsOutputGroupPos = appendPayloadsToNewGroup(result, flatPayloads);
            result.flattenGroup(flatPayloadsOutputGroupPos);
        }
        for (auto& payloadGroupPos : inputSchema.getGroupsPosInScope()) {
            auto payloadGroup = inputSchema.getGroup(payloadGroupPos);
            if (!payloadGroup->getIsFlat()) {
                auto payloads = inputSchema.getExpressionsInScope(payloadGroupPos);
                auto outputGroupPos = appendPayloadsToNewGroup(result, payloads);
                result.getGroup(outputGroupPos)->setMultiplier(payloadGroup->getMultiplier());
            }
        }
    }
}

void SinkOperatorUtil::recomputeSchema(const Schema& inputSchema, Schema& result) {
    assert(!inputSchema.getExpressionsInScope().empty());
    result.clear();
    mergeSchema(inputSchema, result);
}

unordered_set<uint32_t> SinkOperatorUtil::getGroupsPosIgnoringKeyGroups(
    const Schema& schema, const vector<string>& keys) {
    auto payloadGroupsPos = schema.getGroupsPosInScope();
    for (auto& key : keys) {
        auto keyGroupPos = schema.getGroupPos(key);
        payloadGroupsPos.erase(keyGroupPos);
    }
    return payloadGroupsPos;
}

void SinkOperatorUtil::mergeKeyGroup(const Schema& inputSchema, Schema& resultSchema,
    uint32_t keyGroupPos, const vector<string>& keysInGroup) {
    auto resultKeyGroupPos = resultSchema.getGroupPos(keysInGroup[0]);
    for (auto& expression : inputSchema.getExpressionsInScope(keyGroupPos)) {
        if (find(keysInGroup.begin(), keysInGroup.end(), expression->getUniqueName()) !=
            keysInGroup.end()) {
            continue;
        }
        resultSchema.insertToGroupAndScope(expression, resultKeyGroupPos);
    }
}

expression_vector SinkOperatorUtil::getFlatPayloads(
    const Schema& schema, const unordered_set<uint32_t>& payloadGroupsPos) {
    expression_vector result;
    for (auto& payloadGroupPos : payloadGroupsPos) {
        if (schema.getGroup(payloadGroupPos)->getIsFlat()) {
            for (auto& payload : schema.getExpressionsInScope(payloadGroupPos)) {
                result.push_back(payload);
            }
        }
    }
    return result;
}

bool SinkOperatorUtil::hasUnFlatPayload(
    const Schema& schema, const unordered_set<uint32_t>& payloadGroupsPos) {
    for (auto& payloadGroupPos : payloadGroupsPos) {
        if (!schema.getGroup(payloadGroupPos)->getIsFlat()) {
            return true;
        }
    }
    return false;
}

uint32_t SinkOperatorUtil::appendPayloadsToNewGroup(Schema& schema, expression_vector& payloads) {
    auto outputGroupPos = schema.createGroup();
    for (auto& payload : payloads) {
        schema.insertToGroupAndScope(payload, outputGroupPos);
    }
    return outputGroupPos;
}

} // namespace planner
} // namespace kuzu
