#pragma once

#include "schema.h"

namespace kuzu {
namespace planner {
using namespace kuzu::binder;

// This class contains the logic for re-computing factorization structure after
class SinkOperatorUtil {
public:
    static void mergeSchema(const Schema& inputSchema, Schema& result, const vector<string>& keys);

    static void mergeSchema(const Schema& inputSchema, Schema& result);

    static void recomputeSchema(const Schema& inputSchema, Schema& result);

    static unordered_set<uint32_t> getGroupsPosIgnoringKeyGroups(
        const Schema& schema, const vector<string>& keys);

private:
    static void mergeKeyGroup(const Schema& inputSchema, Schema& resultSchema, uint32_t keyGroupPos,
        const vector<string>& keysInGroup);

    static inline expression_vector getFlatPayloadsIgnoringKeyGroup(
        const Schema& schema, const vector<string>& keys) {
        return getFlatPayloads(schema, getGroupsPosIgnoringKeyGroups(schema, keys));
    }
    static inline expression_vector getFlatPayloads(const Schema& schema) {
        return getFlatPayloads(schema, schema.getGroupsPosInScope());
    }
    static expression_vector getFlatPayloads(
        const Schema& schema, const unordered_set<uint32_t>& payloadGroupsPos);

    static inline bool hasUnFlatPayload(const Schema& schema) {
        return hasUnFlatPayload(schema, schema.getGroupsPosInScope());
    }
    static bool hasUnFlatPayload(
        const Schema& schema, const unordered_set<uint32_t>& payloadGroupsPos);

    static uint32_t appendPayloadsToNewGroup(Schema& schema, expression_vector& payloads);
};

} // namespace planner
} // namespace kuzu
