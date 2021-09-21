#pragma once

#include "src/processor/include/physical_plan/operator/physical_operator.h"

namespace graphflow {
namespace processor {

class MultiplicityReducer : public PhysicalOperator {

public:
    MultiplicityReducer(
        unique_ptr<PhysicalOperator> prevOperator, ExecutionContext& context, uint32_t id)
        : PhysicalOperator{move(prevOperator), MULTIPLICITY_REDUCER, context, id},
          prevMultiplicity{1}, numRepeat{0} {}

    void reInitialize() override;

    bool getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<MultiplicityReducer>(prevOperator->clone(), context, id);
    }

private:
    inline void restoreMultiplicity() { resultSet->multiplicity = prevMultiplicity; }

    inline void saveMultiplicity() { prevMultiplicity = resultSet->multiplicity; }

private:
    uint64_t prevMultiplicity;
    uint64_t numRepeat;
};

} // namespace processor
} // namespace graphflow
