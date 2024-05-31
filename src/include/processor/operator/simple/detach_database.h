#pragma once

#include "simple.h"

namespace kuzu {
namespace processor {

class DetachDatabase final : public Simple {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::DETACH_DATABASE;

public:
    DetachDatabase(std::string dbName, const DataPos& outputPos, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : Simple{type_, outputPos, id, std::move(printInfo)}, dbName{std::move(dbName)} {}

    void executeInternal(ExecutionContext* context) final;
    std::string getOutputMsg() final;

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<DetachDatabase>(dbName, outputPos, id, printInfo->copy());
    }

private:
    std::string dbName;
};

} // namespace processor
} // namespace kuzu
