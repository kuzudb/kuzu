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
public:
    Profile(DataPos outputPos, ProfileInfo info, ProfileLocalState localState, uint32_t id,
        const std::string& paramsString, std::unique_ptr<PhysicalOperator> child)
        : PhysicalOperator{PhysicalOperatorType::PROFILE, std::move(child), id, paramsString},
          outputPos{outputPos}, info{info}, localState{localState} {}

    inline bool isSource() const override { return true; }
    inline bool canParallel() const final { return false; }

    inline void setPhysicalPlan(PhysicalPlan* physicalPlan) { info.physicalPlan = physicalPlan; }

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool getNextTuplesInternal(ExecutionContext* context) override;

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<Profile>(outputPos, info, localState, id, paramsString,
            children[0]->clone());
    }

private:
    DataPos outputPos;
    ProfileInfo info;
    ProfileLocalState localState;
    common::ValueVector* outputVector;
};

} // namespace processor
} // namespace kuzu
