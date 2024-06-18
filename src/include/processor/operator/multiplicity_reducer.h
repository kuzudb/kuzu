#pragma once

#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

class MultiplicityReducer : public PhysicalOperator {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::MULTIPLICITY_REDUCER;

public:
    MultiplicityReducer(std::unique_ptr<PhysicalOperator> child, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : PhysicalOperator{type_, std::move(child), id, std::move(printInfo)}, prevMultiplicity{1},
          numRepeat{0} {}

    bool getNextTuplesInternal(ExecutionContext* context) override;

    std::unique_ptr<PhysicalOperator> clone() override {
        return make_unique<MultiplicityReducer>(children[0]->clone(), id, printInfo->copy());
    }

private:
    void restoreMultiplicity() { resultSet->multiplicity = prevMultiplicity; }

    void saveMultiplicity() { prevMultiplicity = resultSet->multiplicity; }

private:
    uint64_t prevMultiplicity;
    uint64_t numRepeat;
};

} // namespace processor
} // namespace kuzu
