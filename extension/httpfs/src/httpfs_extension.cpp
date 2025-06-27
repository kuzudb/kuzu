#include "main/httpfs_extension.h"

#include "common/types/types.h"
#include "common/types/value/value.h"
#include "http_config.h"
#include "main/database.h"
#include "s3fs.h"
#include "s3fs_config.h"

namespace kuzu {
namespace httpfs_extension {

static void registerExtensionOptions(main::Database* db) {
    for (auto& fsConfig : S3FileSystemConfig::getAvailableConfigs()) {
        fsConfig.registerExtensionOptions(db);
    }
    db->addExtensionOption("s3_uploader_max_num_parts_per_file", common::LogicalTypeID::INT64,
        common::Value{static_cast<int64_t>(800000000000)});
    db->addExtensionOption("s3_uploader_max_filesize", common::LogicalTypeID::INT64,
        common::Value{static_cast<int64_t>(10000)});
    db->addExtensionOption("s3_uploader_threads_limit", common::LogicalTypeID::INT64,
        common::Value{static_cast<int64_t>(50)});
    db->addExtensionOption(HTTPCacheFileConfig::HTTP_CACHE_FILE_OPTION, common::LogicalTypeID::BOOL,
        common::Value{HTTPCacheFileConfig::DEFAULT_CACHE_FILE});
}

static void registerFileSystem(main::Database* db) {
    db->registerFileSystem(std::make_unique<HTTPFileSystem>());
    for (auto& fsConfig : S3FileSystemConfig::getAvailableConfigs()) {
        db->registerFileSystem(std::make_unique<S3FileSystem>(fsConfig));
    }
}

void HttpfsExtension::load(main::ClientContext* context) {
    auto db = context->getDatabase();
    registerFileSystem(db);
    registerExtensionOptions(db);
    for (auto& fsConfig : S3FileSystemConfig::getAvailableConfigs()) {
        fsConfig.setEnvValue(context);
    }
    HTTPConfigEnvProvider::setOptionValue(context);
}

} // namespace httpfs_extension
} // namespace kuzu

#if defined(BUILD_DYNAMIC_LOAD)
extern "C" {
// Because we link against the static library on windows, we implicitly inherit KUZU_STATIC_DEFINE,
// which cancels out any exporting, so we can't use KUZU_API.
#if defined(_WIN32)
#define INIT_EXPORT __declspec(dllexport)
#else
#define INIT_EXPORT __attribute__((visibility("default")))
#endif
INIT_EXPORT void init(kuzu::main::ClientContext* context) {
    kuzu::httpfs_extension::HttpfsExtension::load(context);
}

INIT_EXPORT const char* name() {
    return kuzu::httpfs_extension::HttpfsExtension::EXTENSION_NAME;
}
}
#endif
