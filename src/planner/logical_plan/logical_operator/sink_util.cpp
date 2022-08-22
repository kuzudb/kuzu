#include "include/sink_util.h"

namespace graphflow {
namespace planner {

void SinkOperatorUtil::mergeSchema(
    const Schema& inputSchema, Schema& result, const string& key, bool isScanOneRow) {
    mergeKeyGroup(inputSchema, result, key);
    if (getGroupsPosIgnoringKeyGroup(inputSchema, key).empty()) { // nothing else to merge
        return;
    }
    auto flatPayloads = getFlatPayloadsIgnoringKeyGroup(inputSchema, key);
    if (!flatPayloads.empty()) {
        auto flatPayloadsOutputGroupPos = appendPayloadsToNewGroup(result, flatPayloads);
        if (isScanOneRow) {
            result.flattenGroup(flatPayloadsOutputGroupPos);
        }
    }
    for (auto& payloadGroupPos : getGroupsPosIgnoringKeyGroup(inputSchema, key)) {
        auto payloadGroup = inputSchema.getGroup(payloadGroupPos);
        if (!payloadGroup->getIsFlat()) {
            auto payloads = inputSchema.getExpressionsInScope(payloadGroupPos);
            auto outputGroupPos = appendPayloadsToNewGroup(result, payloads);
            result.getGroup(outputGroupPos)->setMultiplier(payloadGroup->getMultiplier());
        }
    }
}

void SinkOperatorUtil::reComputeSchema(const Schema& inputSchema, Schema& result) {
    assert(!inputSchema.getExpressionsInScope().empty());
    result.clear();
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

unordered_set<uint32_t> SinkOperatorUtil::getGroupsPosIgnoringKeyGroup(
    const Schema& schema, const string& key) {
    auto keyGroupPos = schema.getGroupPos(key);
    auto payloadGroupsPos = schema.getGroupsPosInScope();
    payloadGroupsPos.erase(keyGroupPos);
    return payloadGroupsPos;
}

void SinkOperatorUtil::mergeKeyGroup(
    const Schema& inputSchema, Schema& resultSchema, const string& key) {
    auto inputKeyGroupPos = inputSchema.getGroupPos(key);
    auto resultKeyGroupPos = resultSchema.getGroupPos(key);
    for (auto& expression : inputSchema.getExpressionsInScope(inputKeyGroupPos)) {
        if (expression->getUniqueName() == key) {
            continue;
        }
        resultSchema.insertToGroupAndScope(expression, resultKeyGroupPos);
    }
}

expression_vector SinkOperatorUtil::getFlatPayloads(
    const Schema& schema, unordered_set<uint32_t> payloadGroupsPos) {
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
    const Schema& schema, unordered_set<uint32_t> payloadGroupsPos) {
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
} // namespace graphflow
