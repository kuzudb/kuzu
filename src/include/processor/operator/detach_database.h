#pragma once

#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

class DetachDatabase final : public PhysicalOperator {
public:
    DetachDatabase(std::string dbName, uint32_t id, const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::DETACH_DATABASE, id, paramsString},
          dbName{std::move(dbName)} {}

    bool isSource() const override { return true; }
    bool canParallel() const override { return false; }

    bool getNextTuplesInternal(ExecutionContext* context) override;

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<DetachDatabase>(dbName, id, paramsString);
    }

private:
    std::string dbName;
};

} // namespace processor
} // namespace kuzu
