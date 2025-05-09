#pragma once

#include "extension/extension_installer.h"
#include "extension_print_info.h"
#include "processor/operator/simple/simple.h"

namespace kuzu {
namespace processor {

struct InstallExtensionPrintInfo final : public ExtensionPrintInfo {
    explicit InstallExtensionPrintInfo(std::string extensionName)
        : ExtensionPrintInfo{std::move(extensionName)} {}

    std::string toString() const override { return "Install " + extensionName; }

    std::unique_ptr<OPPrintInfo> copy() const override {
        return std::make_unique<InstallExtensionPrintInfo>(*this);
    }
};

class InstallExtension final : public Simple {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::INSTALL_EXTENSION;

public:
    InstallExtension(extension::InstallExtensionInfo info, const DataPos& outputPos, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : Simple{type_, outputPos, id, std::move(printInfo)}, info{std::move(info)} {}

    void executeInternal(ExecutionContext* context) override;
    std::string getOutputMsg() override;

    std::unique_ptr<PhysicalOperator> copy() override {
        return std::make_unique<InstallExtension>(info, outputPos, id, printInfo->copy());
    }

private:
    void setOutputMessage(bool installed);

private:
    extension::InstallExtensionInfo info;
    std::string outputMessage;
};

} // namespace processor
} // namespace kuzu
