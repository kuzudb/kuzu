#pragma once

#include <cstdint>
#include <memory>

namespace kuzu {
namespace extension {

enum class ExtensionAction : uint8_t {
    INSTALL = 0,
    LOAD = 1,
};

struct ExtensionAuxInfo {
    ExtensionAction action;
    std::string path;

    ExtensionAuxInfo(ExtensionAction action, std::string path)
        : action{action}, path{std::move(path)} {}

    virtual ~ExtensionAuxInfo() = default;

    template<typename TARGET>
    const TARGET& contCast() const {
        return dynamic_cast<const TARGET&>(*this);
    }

    virtual std::unique_ptr<ExtensionAuxInfo> copy() {
        return std::make_unique<ExtensionAuxInfo>(*this);
    }
};

struct InstallExtensionAuxInfo : public ExtensionAuxInfo {
    std::string extensionRepo;

    explicit InstallExtensionAuxInfo(std::string extensionRepo, std::string path)
        : ExtensionAuxInfo{ExtensionAction::INSTALL, std::move(path)},
          extensionRepo{std::move(extensionRepo)} {}

    std::unique_ptr<ExtensionAuxInfo> copy() override {
        return std::make_unique<InstallExtensionAuxInfo>(*this);
    }
};

} // namespace extension
} // namespace kuzu
