#pragma once

#include "common/types/value/value.h"

namespace kuzu {
namespace main {
class Database;
class ClientContext;
} // namespace main

namespace httpfs {

struct S3DownloadOptions {
    static void registerExtensionOptions(main::Database* db);
};

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

struct S3EnvironmentCredentialsProvider {
    static constexpr const char* S3_ACCESS_KEY_ENV_VAR = "S3_ACCESS_KEY_ID";
    static constexpr const char* S3_SECRET_KEY_ENV_VAR = "S3_SECRET_ACCESS_KEY";
    static constexpr const char* S3_ENDPOINT_ENV_VAR = "S3_ENDPOINT";
    static constexpr const char* S3_URL_STYLE_ENV_VAR = "S3_URL_STYLE";
    static constexpr const char* S3_REGION_ENV_VAR = "S3_REGION";

    static void setOptionValue(main::ClientContext* context);
};

} // namespace httpfs
} // namespace kuzu
