#pragma once

#include "anti_join_hash_table.h"
#include "hash_join_probe.h"

namespace kuzu {
namespace processor {

// Probe side on left, i.e. children[0] and build side on right, i.e. children[1]
class AntiJoinProbe : public HashJoinProbe {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::ANTI_JOIN_PROBE;

public:
    AntiJoinProbe(std::shared_ptr<HashJoinSharedState> sharedState, common::JoinType joinType,
        bool flatProbe, const ProbeDataInfo& probeDataInfo,
        std::unique_ptr<PhysicalOperator> probeChild, std::unique_ptr<PhysicalOperator> buildChild,
        uint32_t id, std::unique_ptr<OPPrintInfo> printInfo)
        : HashJoinProbe{std::move(sharedState), joinType, flatProbe, probeDataInfo,
              std::move(probeChild), std::move(buildChild), id, std::move(printInfo)} {}

    // This constructor is used for cloning only.
    AntiJoinProbe(std::shared_ptr<HashJoinSharedState> sharedState, common::JoinType joinType,
        bool flatProbe, const ProbeDataInfo& probeDataInfo,
        std::unique_ptr<PhysicalOperator> probeChild, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : HashJoinProbe{std::move(sharedState), joinType, flatProbe, probeDataInfo,
              std::move(probeChild), id, std::move(printInfo)} {}

    std::unique_ptr<PhysicalOperator> clone() override {
        return make_unique<AntiJoinProbe>(sharedState, joinType, flatProbe, probeDataInfo,
            children[0]->clone(), id, printInfo->copy());
    }

protected:
    bool getMatchedTuples(ExecutionContext* context) override;

private:
    void moveProbedTuplesToMatched();

private:
    AntiJoinProbeState antiJoinProbeState;
};

} // namespace processor
} // namespace kuzu
