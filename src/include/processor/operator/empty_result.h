#pragma once

#include "physical_operator.h"

namespace kuzu {
namespace processor {

class EmptyResult final : public PhysicalOperator {
public:
    EmptyResult(uint32_t id, const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::EMPTY_RESULT, id, paramsString} {}

    bool isSource() const override { return true; }

    bool getNextTuplesInternal(ExecutionContext*) override;

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<EmptyResult>(id, paramsString);
    }
};

} // namespace processor
} // namespace kuzu
