#pragma once

#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

struct ProfileInfo {
    PhysicalPlan* physicalPlan;
};

struct ProfileLocalState {
    bool hasExecuted = false;
};

class Profile : public PhysicalOperator {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::PROFILE;

public:
    Profile(DataPos outputPos, ProfileInfo info, ProfileLocalState localState, uint32_t id,
        std::unique_ptr<PhysicalOperator> child, std::unique_ptr<OPPrintInfo> printInfo)
        : PhysicalOperator{type_, std::move(child), id, std::move(printInfo)}, outputPos{outputPos},
          info{info}, localState{localState} {}

    bool isSource() const override { return true; }
    bool isParallel() const final { return false; }

    inline void setPhysicalPlan(PhysicalPlan* physicalPlan) { info.physicalPlan = physicalPlan; }

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool getNextTuplesInternal(ExecutionContext* context) override;

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<Profile>(outputPos, info, localState, id, children[0]->clone(),
            printInfo->copy());
    }

private:
    DataPos outputPos;
    ProfileInfo info;
    ProfileLocalState localState;
    common::ValueVector* outputVector;
};

} // namespace processor
} // namespace kuzu
