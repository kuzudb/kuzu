#include "httpfs_extension.h"

#include "common/types/types.h"
#include "common/types/value/value.h"
#include "s3fs.h"

namespace kuzu {
namespace httpfs {

void HttpfsExtension::load(main::Database& db) {
    db.registerFileSystem(std::make_unique<HTTPFileSystem>());
    db.registerFileSystem(std::make_unique<S3FileSystem>());
    db.addExtensionOption("s3_access_key_id", common::LogicalTypeID::STRING, common::Value{""});
    db.addExtensionOption("s3_secret_access_key", common::LogicalTypeID::STRING, common::Value{""});
    db.addExtensionOption("s3_endpoint", common::LogicalTypeID::STRING, common::Value{""});
    db.addExtensionOption("s3_url_style", common::LogicalTypeID::STRING, common::Value{""});
    db.addExtensionOption("s3_region", common::LogicalTypeID::STRING, common::Value{""});
}

} // namespace httpfs
} // namespace kuzu

extern "C" {
void init(kuzu::main::Database& db) {
    kuzu::httpfs::HttpfsExtension::load(db);
}
}
