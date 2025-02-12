#include "main/duckdb_extension.h"

#include "connector/duckdb_connector.h"
#include "connector/duckdb_secret_manager.h"
#include "main/client_context.h"
#include "s3fs_config.h"
#include "storage/duckdb_storage.h"

namespace kuzu {
namespace duckdb_extension {

void DuckDBExtension::load(main::ClientContext* context) {
    auto db = context->getDatabase();
    db->registerStorageExtension(EXTENSION_NAME, std::make_unique<DuckDBStorageExtension>(db));
    DuckDBExtension::loadRemoteFSOptions(context);
}

void DuckDBExtension::loadRemoteFSOptions(main::ClientContext* context) {
    auto& db = *context->getDatabase();
    for (auto& fsConfig : httpfs::S3FileSystemConfig::getAvailableConfigs()) {
        fsConfig.registerExtensionOptions(&db);
        fsConfig.setEnvValue(context);
    }
}

void DuckDBExtension::initRemoteFSSecrets(duckdb_extension::DuckDBConnector& connector,
    main::ClientContext* context) {
    for (auto& fsConfig : httpfs::S3FileSystemConfig::getAvailableConfigs()) {
        connector.executeQuery(DuckDBSecretManager::getRemoteFSSecret(context, fsConfig));
    }
}

} // namespace duckdb_extension
} // namespace kuzu

extern "C" {
// Because we link against the static library on windows, we implicitly inherit KUZU_STATIC_DEFINE,
// which cancels out any exporting, so we can't use KUZU_API.
#if defined(_WIN32)
#define INIT_EXPORT __declspec(dllexport)
#else
#define INIT_EXPORT __attribute__((visibility("default")))
#endif
INIT_EXPORT void init(kuzu::main::ClientContext* context) {
    kuzu::duckdb_extension::DuckDBExtension::load(context);
}

INIT_EXPORT const char* name() {
    return kuzu::duckdb_extension::DuckDBExtension::EXTENSION_NAME;
}
}
