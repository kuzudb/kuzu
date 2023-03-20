#pragma once

#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

class MultiplicityReducer : public PhysicalOperator {

public:
    MultiplicityReducer(
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::MULTIPLICITY_REDUCER, std::move(child), id,
              paramsString},
          prevMultiplicity{1}, numRepeat{0} {}

    bool getNextTuplesInternal(ExecutionContext* context) override;

    std::unique_ptr<PhysicalOperator> clone() override {
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
