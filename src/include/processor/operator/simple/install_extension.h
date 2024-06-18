#pragma once

#include "processor/operator/simple/simple.h"

namespace kuzu {
namespace processor {

class InstallExtension final : public Simple {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::INSTALL_EXTENSION;

public:
    InstallExtension(std::string name, const DataPos& outputPos, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : Simple{type_, outputPos, id, std::move(printInfo)}, name{std::move(name)} {}

    void executeInternal(ExecutionContext* context) override;
    std::string getOutputMsg() override;

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<InstallExtension>(name, outputPos, id, printInfo->copy());
    }

private:
    std::string tryDownloadExtension();

    void saveExtensionToLocalFile(const std::string& extensionData, main::ClientContext* context);

    void installExtension(main::ClientContext* context);

private:
    std::string name;
};

} // namespace processor
} // namespace kuzu
