#include "duckdb_s3_auth.h"

namespace kuzu {
namespace duckdb_extension {

using namespace common;

void DuckDBS3EnvironmentCredentialsProvider::setOptionValue(main::ClientContext* context) {
    auto accessKeyID = context->getEnvVariable(DUCK_DB_S3_ACCESS_KEY_ENV_VAR);
    auto secretAccessKey = context->getEnvVariable(DUCK_DB_S3_SECRET_KEY_ENV_VAR);
    auto endpoint = context->getEnvVariable(DUCK_DB_S3_ENDPOINT_ENV_VAR);
    auto urlStyle = context->getEnvVariable(DUCK_DB_S3_URL_STYLE_ENV_VAR);
    auto region = context->getEnvVariable(DUCK_DB_S3_REGION_ENV_VAR);
    if (accessKeyID != "") {
        context->setExtensionOption("s3_access_key_id", Value::createValue(accessKeyID));
    }
    if (secretAccessKey != "") {
        context->setExtensionOption("s3_secret_access_key", Value::createValue(secretAccessKey));
    }
    if (endpoint != "") {
        context->setExtensionOption("s3_endpoint", Value::createValue(endpoint));
    }
    if (urlStyle != "") {
        context->setExtensionOption("s3_url_style", Value::createValue(urlStyle));
    }
    if (region != "") {
        context->setExtensionOption("s3_region", Value::createValue(region));
    }
}

} // namespace duckdb_extension
} // namespace kuzu
