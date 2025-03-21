#include "main/client_context.h"
#include "main/duckdb_extension.h"
#include "s3fs_config.h"

namespace kuzu {
namespace duckdb_extension {

void DuckdbExtension::loadRemoteFSOptions(main::ClientContext* context) {
    auto& db = *context->getDatabase();
    for (auto& fsConfig : httpfs_extension::S3FileSystemConfig::getAvailableConfigs()) {
        fsConfig.registerExtensionOptions(&db);
        fsConfig.setEnvValue(context);
    }
}

} // namespace duckdb_extension
} // namespace kuzu
