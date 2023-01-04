#pragma once

#include "schema.h"

namespace kuzu {
namespace planner {

// This class contains the logic for re-computing factorization structure after sinking
class SinkOperatorUtil {
public:
    static void mergeSchema(const Schema& inputSchema, const expression_vector& expressionsToMerge,
        Schema& resultSchema);

    static void recomputeSchema(const Schema& inputSchema,
        const expression_vector& expressionsToMerge, Schema& resultSchema);

private:
    static unordered_map<f_group_pos, expression_vector> getUnFlatPayloadsPerGroup(
        const Schema& schema, const expression_vector& payloads);

    static expression_vector getFlatPayloads(
        const Schema& schema, const expression_vector& payloads);

    static uint32_t appendPayloadsToNewGroup(Schema& schema, expression_vector& payloads);
};

} // namespace planner
} // namespace kuzu
