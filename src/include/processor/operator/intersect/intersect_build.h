#pragma once

#include "processor/operator/hash_join/hash_join_build.h"

namespace kuzu {
namespace processor {

class IntersectBuild : public HashJoinBuild {
public:
    IntersectBuild(std::unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        std::shared_ptr<HashJoinSharedState> sharedState, std::unique_ptr<HashJoinBuildInfo> info,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : HashJoinBuild{std::move(resultSetDescriptor), PhysicalOperatorType::INTERSECT_BUILD,
              std::move(sharedState), std::move(info), std::move(child), id, paramsString} {}

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return make_unique<IntersectBuild>(resultSetDescriptor->copy(), sharedState, info->copy(),
            children[0]->clone(), id, paramsString);
    }

    inline void appendVectors() final {
        KU_ASSERT(keyVectors.size() == 1);
        hashTable->appendVectorWithSorting(keyVectors[0], payloadVectors);
    }
};

} // namespace processor
} // namespace kuzu
