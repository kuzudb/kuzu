#pragma once

#include "extension/extension_loader.h"

namespace kuzu {
namespace duckdb_extension {

class DuckDBLoader final : public extension::ExtensionLoader {
public:
    explicit DuckDBLoader(std::string extensionName)
        : extension::ExtensionLoader{std::move(extensionName)} {}

    void loadDependency(main::ClientContext* context) override;
};

} // namespace duckdb_extension
} // namespace kuzu
