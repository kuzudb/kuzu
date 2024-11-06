#include "s3_download_options.h"

#include "main/client_context.h"
#include "main/database.h"

namespace kuzu {
namespace httpfs {

using namespace common;

#define ADD_EXTENSION_OPTION(OPTION)                                                               \
    db->addExtensionOption(OPTION::NAME, OPTION::TYPE, OPTION::getDefaultValue())

void S3DownloadOptions::registerExtensionOptions(main::Database* db) {
    ADD_EXTENSION_OPTION(S3AccessKeyID);
    ADD_EXTENSION_OPTION(S3SecretAccessKey);
    ADD_EXTENSION_OPTION(S3EndPoint);
    ADD_EXTENSION_OPTION(S3URLStyle);
    ADD_EXTENSION_OPTION(S3Region);
}

void S3EnvironmentCredentialsProvider::setOptionValue(main::ClientContext* context) {
    auto accessKeyID = context->getEnvVariable(S3_ACCESS_KEY_ENV_VAR);
    auto secretAccessKey = context->getEnvVariable(S3_SECRET_KEY_ENV_VAR);
    auto endpoint = context->getEnvVariable(S3_ENDPOINT_ENV_VAR);
    auto urlStyle = context->getEnvVariable(S3_URL_STYLE_ENV_VAR);
    auto region = context->getEnvVariable(S3_REGION_ENV_VAR);
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

} // namespace httpfs
} // namespace kuzu
