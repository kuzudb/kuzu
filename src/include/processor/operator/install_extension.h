#pragma once

#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

class InstallExtension final : public PhysicalOperator {
public:
    InstallExtension(std::string name, uint32_t id, const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::INSTALL_EXTENSION, id, paramsString},
          name{std::move(name)}, hasExecuted{false} {}

    inline bool isSource() const override { return true; }
    inline bool canParallel() const override { return false; }

    bool getNextTuplesInternal(ExecutionContext* context) override;

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<InstallExtension>(name, id, paramsString);
    }

private:
    std::string tryDownloadExtension();

    void saveExtensionToLocalFile(
        const std::string& extensionData, common::VirtualFileSystem* vfs, main::Database* database);

    void installExtension(common::VirtualFileSystem* vfs, main::Database* database);

private:
    std::string name;
    bool hasExecuted;
};

} // namespace processor
} // namespace kuzu
