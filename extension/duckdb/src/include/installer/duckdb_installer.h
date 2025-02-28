#pragma once

#include "extension/extension_installer.h"

namespace kuzu {
namespace duckdb_extension {

class KUZU_API DuckDBInstaller final : public extension::ExtensionInstaller {
public:
    DuckDBInstaller(const extension::InstallExtensionInfo& info, main::ClientContext& context)
        : ExtensionInstaller{info, context} {}

    void install() override;
};

} // namespace duckdb_extension
} // namespace kuzu
