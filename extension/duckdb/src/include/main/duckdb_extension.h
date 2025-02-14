#pragma once

#include "extension/extension.h"
#include "main/client_context.h"
#include "s3fs_config.h"

namespace kuzu {
namespace duckdb_extension {

class DuckDBConnector;

class DuckDBExtension final : public extension::Extension {
public:
    static constexpr char EXTENSION_NAME[] = "DUCKDB";
    static constexpr const char* DEPENDENCY_LIB_FILES[] = {"libduckdb"};

public:
    static void load(main::ClientContext* context);
    static void loadRemoteFSOptions(main::ClientContext* context) {
        auto& db = *context->getDatabase();
        for (auto& fsConfig : httpfs::S3FileSystemConfig::getAvailableConfigs()) {
            fsConfig.registerExtensionOptions(&db);
            fsConfig.setEnvValue(context);
        }
    }
};

} // namespace duckdb_extension
} // namespace kuzu
