#pragma once
#include "extension/extension_installer.h"

namespace kuzu {
namespace duckdb_extension {

class KUZU_API DuckDBInstaller : public extension::ExtensionInstaller {
public:
    explicit DuckDBInstaller(const std::string extensionName)
        : ExtensionInstaller{std::move(extensionName)} {}

    void install(main::ClientContext* context) override;
};

} // namespace duckdb_extension
} // namespace kuzu
