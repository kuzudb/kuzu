#pragma once

#include "processor/operator/hash_join/hash_join_build.h"

namespace kuzu {
namespace processor {

class IntersectBuild : public HashJoinBuild {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::INTERSECT_BUILD;

public:
    IntersectBuild(std::unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        std::shared_ptr<HashJoinSharedState> sharedState, std::unique_ptr<HashJoinBuildInfo> info,
        std::unique_ptr<PhysicalOperator> child, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : HashJoinBuild{std::move(resultSetDescriptor), type_, std::move(sharedState),
              std::move(info), std::move(child), id, std::move(printInfo)} {}

    std::unique_ptr<PhysicalOperator> clone() override {
        return make_unique<IntersectBuild>(resultSetDescriptor->copy(), sharedState, info->copy(),
            children[0]->clone(), id, printInfo->copy());
    }

    void appendVectors() final {
        KU_ASSERT(keyVectors.size() == 1);
        hashTable->appendVectorWithSorting(keyVectors[0], payloadVectors);
    }
};

} // namespace processor
} // namespace kuzu
