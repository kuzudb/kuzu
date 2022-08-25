#pragma once

#include "schema.h"

namespace graphflow {
namespace planner {
using namespace graphflow::binder;

// This class contains the logic for re-computing factorization structure after
class SinkOperatorUtil {
public:
    static void mergeSchema(
        const Schema& inputSchema, Schema& result, const string& key, bool isScanOneRow);

    static void reComputeSchema(const Schema& inputSchema, Schema& result);

    static unordered_set<uint32_t> getGroupsPosIgnoringKeyGroup(
        const Schema& schema, const string& key);

private:
    static void mergeKeyGroup(const Schema& inputSchema, Schema& resultSchema, const string& key);

    static inline expression_vector getFlatPayloadsIgnoringKeyGroup(
        const Schema& schema, const string& key) {
        return getFlatPayloads(schema, getGroupsPosIgnoringKeyGroup(schema, key));
    }
    static inline expression_vector getFlatPayloads(const Schema& schema) {
        return getFlatPayloads(schema, schema.getGroupsPosInScope());
    }
    static expression_vector getFlatPayloads(
        const Schema& schema, unordered_set<uint32_t> payloadGroupsPos);

    static inline bool hasUnflatPayloadIgnoringKeyGroup(const Schema& schema, const string& key) {
        return hasUnFlatPayload(schema, getGroupsPosIgnoringKeyGroup(schema, key));
    }
    static inline bool hasUnFlatPayload(const Schema& schema) {
        return hasUnFlatPayload(schema, schema.getGroupsPosInScope());
    }
    static bool hasUnFlatPayload(const Schema& schema, unordered_set<uint32_t> payloadGroupsPos);

    static uint32_t appendPayloadsToNewGroup(Schema& schema, expression_vector& payloads);
};

} // namespace planner
} // namespace graphflow
