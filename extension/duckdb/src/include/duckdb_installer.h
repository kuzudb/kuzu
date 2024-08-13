#pragma once
#include "extension/extension_installer.h"

namespace kuzu {
namespace duckdb {

class DuckDBInstaller : public extension::ExtensionInstaller {
private:
    static constexpr const char* DEPENDENCY_LIB_FILES[] = {"libduckdb.dylib"};

public:
    explicit DuckDBInstaller(const std::string extensionName)
        : ExtensionInstaller{std::move(extensionName)} {}

    void install(main::ClientContext* context) override;
};

} // namespace duckdb
} // namespace kuzu