#pragma once

#include "processor/operator/simple/simple.h"

namespace kuzu {
namespace processor {

struct LoadExtensionPrintInfo final : OPPrintInfo {
    std::string extensionName;

    explicit LoadExtensionPrintInfo(std::string extensionName)
        : extensionName{std::move(extensionName)} {}

    std::string toString() const override;

    std::unique_ptr<OPPrintInfo> copy() const override {
        return std::unique_ptr<LoadExtensionPrintInfo>(new LoadExtensionPrintInfo(*this));
    }

private:
    LoadExtensionPrintInfo(const LoadExtensionPrintInfo& other)
        : OPPrintInfo{other}, extensionName{other.extensionName} {}
};

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
