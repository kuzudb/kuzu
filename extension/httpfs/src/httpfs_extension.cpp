#include "httpfs_extension.h"

#include "common/types/types.h"
#include "common/types/value/value.h"
#include "s3fs.h"

namespace kuzu {
namespace httpfs {

void HttpfsExtension::load(main::ClientContext* context) {
    auto db = context->getDatabase();
    db->registerFileSystem(std::make_unique<HTTPFileSystem>());
    db->registerFileSystem(std::make_unique<S3FileSystem>());
    db->addExtensionOption("s3_access_key_id", common::LogicalTypeID::STRING, common::Value{""});
    db->addExtensionOption(
        "s3_secret_access_key", common::LogicalTypeID::STRING, common::Value{""});
    db->addExtensionOption(
        "s3_endpoint", common::LogicalTypeID::STRING, common::Value{"s3.amazonaws.com"});
    db->addExtensionOption("s3_url_style", common::LogicalTypeID::STRING, common::Value{"vhost"});
    db->addExtensionOption("s3_region", common::LogicalTypeID::STRING, common::Value{"us-east-1"});
    db->addExtensionOption("s3_uploader_max_num_parts_per_file", common::LogicalTypeID::INT64,
        common::Value{(int64_t)800000000000});
    db->addExtensionOption(
        "s3_uploader_max_filesize", common::LogicalTypeID::INT64, common::Value{(int64_t)10000});
    db->addExtensionOption(
        "s3_uploader_threads_limit", common::LogicalTypeID::INT64, common::Value{(int64_t)50});
    AWSEnvironmentCredentialsProvider::setOptionValue(context);
}

} // namespace httpfs
} // namespace kuzu

extern "C" {
void init(kuzu::main::ClientContext* context) {
    kuzu::httpfs::HttpfsExtension::load(context);
}
}
