#include "main/duckdb_extension.h"

#include "main/client_context.h"
#include "s3_download_options.h"
#include "storage/duckdb_storage.h"

namespace kuzu {
namespace duckdb_extension {

void DuckDBExtension::load(main::ClientContext* context) {
    auto db = context->getDatabase();
    db->registerStorageExtension(EXTENSION_NAME, std::make_unique<DuckDBStorageExtension>(db));
    httpfs::S3DownloadOptions::registerExtensionOptions(db);
    httpfs::S3DownloadOptions::setEnvValue(context);
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
