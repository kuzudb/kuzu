#pragma once

#include "main/client_context.h"

namespace kuzu {
namespace duckdb_extension {

struct DuckDBS3EnvironmentCredentialsProvider {
    static constexpr const char* DUCK_DB_S3_ACCESS_KEY_ENV_VAR = "DUCKDB_S3_ACCESS_KEY_ID";
    static constexpr const char* DUCK_DB_S3_SECRET_KEY_ENV_VAR = "DUCKDB_S3_SECRET_ACCESS_KEY";
    static constexpr const char* DUCK_DB_S3_ENDPOINT_ENV_VAR = "DUCKDB_S3_ENDPOINT";
    static constexpr const char* DUCK_DB_S3_URL_STYLE_ENV_VAR = "DUCKDB_S3_URL_STYLE";
    static constexpr const char* DUCK_DB_S3_REGION_ENV_VAR = "DUCKDB_S3_REGION";

    static void setOptionValue(main::ClientContext* context);
};

} // namespace duckdb_extension
} // namespace kuzu
