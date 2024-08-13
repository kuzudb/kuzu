#pragma once
#include "extension/extension_downloader.h"

namespace kuzu {
namespace httpfs {

class HttpfsInstaller : public extension::ExtensionInstaller {
public:
    explicit HttpfsInstaller(const std::string extensionName)
        : ExtensionInstaller{std::move(extensionName)} {}

    void install(main::ClientContext* context) override;
};

} // namespace httpfs
} // namespace kuzu