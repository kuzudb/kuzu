#pragma once

#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

class MultiplicityReducer : public PhysicalOperator {

public:
    MultiplicityReducer(unique_ptr<PhysicalOperator> child, uint32_t id, const string& paramsString)
        : PhysicalOperator{move(child), id, paramsString}, prevMultiplicity{1}, numRepeat{0} {}

    PhysicalOperatorType getOperatorType() override { return MULTIPLICITY_REDUCER; }

    bool getNextTuplesInternal() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<MultiplicityReducer>(children[0]->clone(), id, paramsString);
    }

private:
    inline void restoreMultiplicity() { resultSet->multiplicity = prevMultiplicity; }

    inline void saveMultiplicity() { prevMultiplicity = resultSet->multiplicity; }

private:
    uint64_t prevMultiplicity;
    uint64_t numRepeat;
};

} // namespace processor
} // namespace kuzu
