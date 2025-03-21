#pragma once

#include "extension/extension.h"
#include "main/client_context.h"

namespace kuzu {
namespace duckdb_extension {

class DuckDBConnector;

class DuckdbExtension final : public extension::Extension {
public:
    static constexpr char EXTENSION_NAME[] = "DUCKDB";
    static constexpr const char* DEPENDENCY_LIB_FILES[] = {"libduckdb"};

public:
    static void load(main::ClientContext* context);
    static void loadRemoteFSOptions(main::ClientContext* context);
};

} // namespace duckdb_extension
} // namespace kuzu
