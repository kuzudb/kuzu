#pragma once

#include "physical_operator.h"

namespace kuzu {
namespace processor {

class EmptyResult final : public PhysicalOperator {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::EMPTY_RESULT;

public:
    EmptyResult(uint32_t id, std::unique_ptr<OPPrintInfo> printInfo)
        : PhysicalOperator{type_, id, std::move(printInfo)} {}

    bool isSource() const override { return true; }

    bool getNextTuplesInternal(ExecutionContext*) override;

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<EmptyResult>(id, printInfo->copy());
    }
};

} // namespace processor
} // namespace kuzu
