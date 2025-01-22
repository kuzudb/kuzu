#include "main/delta_extension.h"

#include "function/delta_scan.h"
#include "main/client_context.h"
#include "main/database.h"
#include "s3_download_options.h"

namespace kuzu {
namespace delta_extension {

void DeltaExtension::load(main::ClientContext* context) {
    auto& db = *context->getDatabase();
    extension::ExtensionUtils::addTableFunc<DeltaScanFunction>(db);
    httpfs::S3DownloadOptions::registerExtensionOptions(&db);
    httpfs::S3DownloadOptions::setEnvValue(context);
}

} // namespace delta_extension
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
    kuzu::delta_extension::DeltaExtension::load(context);
}

INIT_EXPORT const char* name() {
    return kuzu::delta_extension::DeltaExtension::EXTENSION_NAME;
}
}
