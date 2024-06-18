#pragma once

#include "processor/operator/simple/simple.h"

namespace kuzu {
namespace processor {

class LoadExtension final : public Simple {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::LOAD_EXTENSION;

public:
    LoadExtension(std::string path, const DataPos& outputPos, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : Simple{type_, outputPos, id, std::move(printInfo)}, path{std::move(path)} {}

    void executeInternal(ExecutionContext* context) override;
    std::string getOutputMsg() override;

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<LoadExtension>(path, outputPos, id, printInfo->copy());
    }

private:
    std::string path;
};

} // namespace processor
} // namespace kuzu
