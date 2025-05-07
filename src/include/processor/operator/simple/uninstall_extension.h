#pragma once

#include "extension_print_info.h"
#include "processor/operator/simple/simple.h"

namespace kuzu {
namespace processor {

struct UninstallExtensionPrintInfo final : public ExtensionPrintInfo {
    explicit UninstallExtensionPrintInfo(std::string extensionName)
        : ExtensionPrintInfo{std::move(extensionName)} {}

    std::string toString() const override { return "Uninstall " + extensionName; }

    std::unique_ptr<OPPrintInfo> copy() const override {
        return std::make_unique<UninstallExtensionPrintInfo>(*this);
    }
};

class UninstallExtension final : public Simple {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::UNINSTALL_EXTENSION;

public:
    UninstallExtension(std::string path, const DataPos& outputPos, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : Simple{type_, outputPos, id, std::move(printInfo)}, path{std::move(path)} {}

    void executeInternal(ExecutionContext* context) override;
    std::string getOutputMsg() override;

    std::unique_ptr<PhysicalOperator> copy() override {
        return std::make_unique<UninstallExtension>(path, outputPos, id, printInfo->copy());
    }

private:
    std::string path;
};

} // namespace processor
} // namespace kuzu
