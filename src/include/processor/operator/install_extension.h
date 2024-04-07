#pragma once

#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

class InstallExtension final : public PhysicalOperator {
public:
    InstallExtension(std::string name, uint32_t id, const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::INSTALL_EXTENSION, id, paramsString},
          name{std::move(name)}, hasExecuted{false} {}

    bool isSource() const override { return true; }
    bool canParallel() const override { return false; }

    bool getNextTuplesInternal(ExecutionContext* context) override;

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<InstallExtension>(name, id, paramsString);
    }

private:
    std::string tryDownloadExtension();

    void saveExtensionToLocalFile(const std::string& extensionData, main::ClientContext* context);

    void installExtension(main::ClientContext* context);

private:
    std::string name;
    bool hasExecuted;
};

} // namespace processor
} // namespace kuzu
