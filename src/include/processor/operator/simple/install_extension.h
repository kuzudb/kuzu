#pragma once

#include "processor/operator/simple/simple.h"

namespace kuzu {
namespace processor {

class InstallExtension final : public Simple {
public:
    InstallExtension(std::string name, const DataPos& outputPos, uint32_t id,
        const std::string& paramsString)
        : Simple{PhysicalOperatorType::INSTALL_EXTENSION, outputPos, id, paramsString},
          name{std::move(name)} {}

    void executeInternal(ExecutionContext* context) override;
    std::string getOutputMsg() override;

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<InstallExtension>(name, outputPos, id, paramsString);
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
