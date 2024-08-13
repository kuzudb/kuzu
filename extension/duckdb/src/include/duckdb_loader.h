#pragma once
#include "extension/extension_loader.h"

namespace kuzu {
namespace duckdb {

class DuckDBLoader final : public extension::ExtensionLoader {
private:
    static constexpr const char* DEPENDENCY_LIB_FILES[] = {"libduckdb.dylib"};

public:
    explicit DuckDBLoader(std::string extensionName)
        : extension::ExtensionLoader{std::move(extensionName)} {}

    void loadDependency(main::ClientContext* context) override;
};

} // namespace duckdb
} // namespace kuzu
