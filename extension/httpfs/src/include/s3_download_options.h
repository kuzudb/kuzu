#pragma once

#include "common/types/value/value.h"

namespace kuzu {
namespace main {
class Database;
class ClientContext;
} // namespace main

namespace httpfs {

struct S3AccessKeyID {
    static constexpr const char* NAME = "S3_ACCESS_KEY_ID";
    static constexpr common::LogicalTypeID TYPE = common::LogicalTypeID::STRING;
    static common::Value getDefaultValue() { return common::Value{""}; }
};

struct S3SecretAccessKey {
    static constexpr const char* NAME = "S3_SECRET_ACCESS_KEY";
    static constexpr common::LogicalTypeID TYPE = common::LogicalTypeID::STRING;
    static common::Value getDefaultValue() { return common::Value{""}; }
};

struct S3EndPoint {
    static constexpr const char* NAME = "S3_ENDPOINT";
    static constexpr common::LogicalTypeID TYPE = common::LogicalTypeID::STRING;
    static common::Value getDefaultValue() { return common::Value{"s3.amazonaws.com"}; }
};

struct S3URLStyle {
    static constexpr const char* NAME = "S3_URL_STYLE";
    static constexpr common::LogicalTypeID TYPE = common::LogicalTypeID::STRING;
    static common::Value getDefaultValue() { return common::Value{"vhost"}; }
};

struct S3Region {
    static constexpr const char* NAME = "S3_REGION";
    static constexpr common::LogicalTypeID TYPE = common::LogicalTypeID::STRING;
    static common::Value getDefaultValue() { return common::Value{"us-east-1"}; }
};

struct S3DownloadOptions {
    static void registerExtensionOptions(main::Database* db);
    static void setEnvValue(main::ClientContext* context);
};

} // namespace httpfs
} // namespace kuzu
